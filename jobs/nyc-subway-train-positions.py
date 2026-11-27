import requests
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from google.transit import gtfs_realtime_pb2
import zipfile
import boto3
from io import BytesIO, StringIO
import sys
import time
import os

# Snapshots where every NYC subway train is right now, from MTA's GTFS-realtime
# feeds (all 8 of them, covering every line including the Staten Island Railway).
# Each train's position is given by the signal system as a station plus a status
# (STOPPED_AT / INCOMING_AT / IN_TRANSIT_TO), not GPS coordinates, so the row
# carries that station's coordinates plus the next stop and its predicted
# arrival, enough to place the train along the line between them.
# The feeds only show the present and are overwritten every ~30 seconds, so these
# snapshots are the history.
# No API key needed (checked 2026-11-27).
# Feed docs: https://api.mta.info/#/subwayRealTimeFeeds

FEED_BASE = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2F'
FEEDS = {  # feed name -> URL suffix; names are the lines each feed covers
    '1234567S': 'gtfs',
    'ACE': 'gtfs-ace',
    'BDFM': 'gtfs-bdfm',
    'G': 'gtfs-g',
    'JZ': 'gtfs-jz',
    'L': 'gtfs-l',
    'NQRW': 'gtfs-nqrw',
    'SIR': 'gtfs-si',
}
STATIC_URL = 'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip'
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; scheduled-jobs data collector)'}
NY = ZoneInfo('America/New_York')

# NYC Transit's extension (field 1001) on TripDescriptor holds the train ID
# (e.g. "1A 2257+ 207/FAR": route, origin time, origin/destination), whether a
# physical train is assigned to the trip, and its direction; on StopTimeUpdate
# it holds the scheduled and actual track. The public bindings don't define it,
# so it is read from the raw protobuf bytes.
NYCT_EXTENSION_FIELD = 1001
DIRECTIONS = {1: 'NORTH', 2: 'EAST', 3: 'SOUTH', 4: 'WEST'}

# fetch a URL, retrying transient failures with backoff
RETRY_DELAYS = [10, 30, 60]  # seconds to wait before each retry
def get(url, name):
    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=60)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            error = e
        else:
            if response.status_code < 500 and response.status_code != 429:
                response.raise_for_status()  # other 4xx means a bad request: fail fast
                return response.content
            error = f'HTTP {response.status_code}'
        if attempt == len(RETRY_DELAYS):
            raise RuntimeError(f'{name} request failed after {attempt + 1} attempts: {error}')
        print(f'{name} attempt {attempt + 1} failed ({error}), retrying in {RETRY_DELAYS[attempt]}s')
        time.sleep(RETRY_DELAYS[attempt])

# minimal protobuf wire-format reader, just enough for the NYCT extension
def read_varint(buf, i):
    result = shift = 0
    while True:
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, i
        shift += 7

def iter_fields(buf):
    i = 0
    while i < len(buf):
        key, i = read_varint(buf, i)
        field, wire_type = key >> 3, key & 7
        if wire_type == 0:
            value, i = read_varint(buf, i)
        elif wire_type == 1:
            value, i = buf[i:i + 8], i + 8
        elif wire_type == 2:
            length, i = read_varint(buf, i)
            value, i = buf[i:i + length], i + length
        elif wire_type == 5:
            value, i = buf[i:i + 4], i + 4
        else:
            raise ValueError(f'unsupported wire type {wire_type}')
        yield field, wire_type, value

def nyct_extension(message):
    # raw sub-fields of the extension; later values win, as in protobuf
    out = {}
    for field, wire_type, value in iter_fields(message.SerializeToString()):
        if field == NYCT_EXTENSION_FIELD and wire_type == 2:
            for inner, _, inner_value in iter_fields(value):
                out[inner] = inner_value
    return out

def trip_extension(trip):
    ext = nyct_extension(trip)
    return {
        'train_id': ext[1].decode('utf-8') if 1 in ext else None,
        # MTA leaves the flag out rather than sending false, so a trip with the
        # extension but no flag has no train assigned yet
        'is_assigned': bool(ext.get(2, 0)) if ext else None,
        'direction': DIRECTIONS.get(ext.get(3)),
    }

def track_extension(stop_time_update):
    ext = nyct_extension(stop_time_update)
    return {
        'scheduled_track': ext[1].decode('utf-8') if 1 in ext else None,
        'actual_track': ext[2].decode('utf-8') if 2 in ext else None,
    }

def to_et(ts):
    return datetime.fromtimestamp(ts, tz=NY).strftime('%Y-%m-%d %H:%M:%S') if ts else None

def to_utc(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if ts else None

def stop_time(stu):
    # predicted time the train reaches a stop: arrival, or departure at an origin
    if stu.HasField('arrival') and stu.arrival.time:
        return stu.arrival.time
    if stu.HasField('departure') and stu.departure.time:
        return stu.departure.time
    return None

# static schedule, for stop names, coordinates and route names
with zipfile.ZipFile(BytesIO(get(STATIC_URL, 'static GTFS'))) as z:
    read = lambda name: pd.read_csv(z.open(name), dtype=str, keep_default_na=False)
    stops = read('stops.txt').set_index('stop_id')
    routes = read('routes.txt')
route_names = dict(zip(routes['route_id'], routes['route_long_name']))

def stop_info(stop_id, prefix):
    if stop_id and stop_id in stops.index:
        s = stops.loc[stop_id]
        return {f'{prefix}_name': s['stop_name'],
                f'{prefix}_lat': float(s['stop_lat']), f'{prefix}_lon': float(s['stop_lon'])}
    return {f'{prefix}_name': None, f'{prefix}_lat': None, f'{prefix}_lon': None}

rows = []
failed_feeds = []
for feed_name, suffix in FEEDS.items():
    try:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(get(FEED_BASE + suffix, f'{feed_name} feed'))
    except Exception as e:
        # keep the other lines' snapshot, and fail the run at the end
        print(f'::error::{feed_name} feed failed: {e}')
        failed_feeds.append(feed_name)
        continue
    feed_ts = feed.header.timestamp
    if not feed_ts:
        print(f'::error::{feed_name} feed has no header timestamp, skipping it')
        failed_feeds.append(feed_name)
        continue

    # a train's trip update and vehicle position are separate entities, joined by trip_id
    trips = {}
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            trips.setdefault(entity.trip_update.trip.trip_id, {})['tu'] = entity.trip_update
        if entity.HasField('vehicle'):
            trips.setdefault(entity.vehicle.trip.trip_id, {})['vp'] = entity.vehicle

    for trip_id, t in trips.items():
        tu, vp = t.get('tu'), t.get('vp')
        trip = (tu or vp).trip
        updates = list(tu.stop_time_update) if tu else []
        next_stu = updates[0] if updates else None
        last_stu = updates[-1] if updates else None
        vehicle_stop = vp.stop_id if vp and vp.stop_id else None
        row = {
            'feed': feed_name,
            'route_id': trip.route_id,
            'route_name': route_names.get(trip.route_id),
            'trip_id': trip_id,
            'start_date': trip.start_date,
            'start_time': trip.start_time,
            **trip_extension(trip),
            'has_vehicle_position': vp is not None,
            # MTA leaves the status out for IN_TRANSIT_TO, the GTFS-realtime default
            'vehicle_status': gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(vp.current_status)
                if vp else None,
            'vehicle_stop_id': vehicle_stop,
            **stop_info(vehicle_stop, 'vehicle_stop'),
            'vehicle_stop_sequence': vp.current_stop_sequence if vp and vp.HasField('current_stop_sequence') else None,
            'vehicle_timestamp_utc': to_utc(vp.timestamp) if vp else None,
            # how stale the position is when the feed was published
            'vehicle_age_sec': feed_ts - vp.timestamp if vp and vp.timestamp else None,
            'next_stop_id': next_stu.stop_id if next_stu else None,
            **stop_info(next_stu.stop_id if next_stu else None, 'next_stop'),
            'next_stop_arrival_et': to_et(stop_time(next_stu)) if next_stu else None,
            **(track_extension(next_stu) if next_stu else {'scheduled_track': None, 'actual_track': None}),
            'terminal_stop_id': last_stu.stop_id if last_stu else None,
            'terminal_stop_name': stop_info(last_stu.stop_id, 't')['t_name'] if last_stu else None,
            'terminal_arrival_et': to_et(stop_time(last_stu)) if last_stu else None,
            'upcoming_stops': len(updates),
            'feed_timestamp_utc': to_utc(feed_ts),
        }
        # the feed also lists trips that haven't left their origin yet (future
        # timestamp, often no train assigned), trains that finished but still
        # sit at the terminal with no stops left, and the odd position that
        # stopped updating hours ago; this flags the rest, the trains actually
        # out on the line. 20 minutes is longer than any normal hold at a station.
        row['in_service'] = bool(
            vp is not None and row['is_assigned'] and updates
            and row['vehicle_age_sec'] is not None and -60 < row['vehicle_age_sec'] <= 20 * 60)
        rows.append(row)
    print(f'{feed_name}: feed at {to_utc(feed_ts)} UTC, {len(trips)} trips, '
          f'{sum(r["in_service"] for r in rows if r["feed"] == feed_name)} in service')

if len(failed_feeds) == len(FEEDS):
    raise RuntimeError('every feed failed; nothing to save')

current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# fixed columns, so a file with no trains running still has the full header
COLUMNS = [
    'feed', 'route_id', 'route_name', 'trip_id', 'start_date', 'start_time',
    'train_id', 'is_assigned', 'direction', 'in_service',
    'has_vehicle_position', 'vehicle_status', 'vehicle_stop_id', 'vehicle_stop_name',
    'vehicle_stop_lat', 'vehicle_stop_lon', 'vehicle_stop_sequence', 'vehicle_timestamp_utc', 'vehicle_age_sec',
    'next_stop_id', 'next_stop_name', 'next_stop_lat', 'next_stop_lon', 'next_stop_arrival_et',
    'scheduled_track', 'actual_track',
    'terminal_stop_id', 'terminal_stop_name', 'terminal_arrival_et', 'upcoming_stops',
    'feed_timestamp_utc',
]
df = pd.DataFrame(rows, columns=COLUMNS)
for col in ['vehicle_stop_sequence', 'vehicle_age_sec', 'upcoming_stops']:
    df[col] = df[col].astype('Int64')
df['datetime'] = current_datetime
print(f'{len(df)} trips across {df["feed"].nunique()} feeds, {int(df["in_service"].sum())} in service')
if len(df):
    print('in service by route:', df[df['in_service']].groupby('route_id')['trip_id'].nunique().to_dict())

if os.environ.get('DRY_RUN'):
    # local check without touching S3: DRY_RUN=1 python jobs/nyc-subway-train-positions.py
    df.to_csv('nyc-subway-train-positions.csv', index=False)
    print('Dry run: wrote nyc-subway-train-positions.csv')
else:
    # get environmental variables
    AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

    # copy to s3; a file is written even when no trains are running, so a quiet
    # hour can be told apart from a missed run
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    def copy_to_s3(client, df, bucket, filepath):
        csv_buf = StringIO()
        df.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)
        client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
        print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

    file_path = 'mta/nyc-subway-train-positions/nyc-subway-train-positions-' + current_datetime_label + '.csv'
    copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath=file_path)

if failed_feeds:
    # the snapshot above has the other lines; fail so the missing ones are noticed
    sys.exit(f'Saved without feeds: {", ".join(failed_feeds)}')
