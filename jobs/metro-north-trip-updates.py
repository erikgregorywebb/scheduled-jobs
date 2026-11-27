import pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from google.transit import gtfs_realtime_pb2
import zipfile
from io import BytesIO

from common import RUN_DATETIME, RUN_LABEL, get as http_get, save_csv

# Snapshots Metro-North's GTFS-realtime trip updates: every train MTA is
# currently tracking, with its predicted arrival/departure at each upcoming stop,
# track, and train status. Each prediction is joined to the static schedule so
# every row carries its own scheduled time and a computed delay in seconds.
# The feed only shows the present and is overwritten continuously, so these
# hourly snapshots build the on-time history (e.g. for the Harlem Line).
# Neither feed needs an API key (checked 2026-11-27).
# Feed docs: https://api.mta.info/#/subwayRealTimeFeeds (Metro-North tab)

RT_URL = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/mnr%2Fgtfs-mnr'
STATIC_URL = 'https://rrgtfsfeeds.s3.amazonaws.com/gtfsmnr.zip'
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; scheduled-jobs data collector)'}
NY = ZoneInfo('America/New_York')

# MTA's railroad extension on StopTimeUpdate (field 1005) holds the track and a
# train status such as "On-Time", "Late" or "Departed". The public bindings
# don't define it, so it is read from the raw protobuf bytes.
MTA_EXTENSION_FIELD = 1005

# fetch a URL's bytes, retrying transient failures with backoff
def get(url, name):
    return http_get(url, headers=HEADERS, timeout=60, name=name).content

# minimal protobuf wire-format reader, just enough for the MTA extension
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

def mta_extension(stop_time_update):
    # the extension can appear more than once; later values win, as in protobuf
    out = {}
    for field, wire_type, value in iter_fields(stop_time_update.SerializeToString()):
        if field == MTA_EXTENSION_FIELD and wire_type == 2:
            for inner, inner_type, inner_value in iter_fields(value):
                if inner_type == 2 and inner in (1, 2):
                    out['track' if inner == 1 else 'train_status'] = inner_value.decode('utf-8')
    return out

# GTFS times are measured from noon minus 12h on the service day (so DST-safe),
# and can run past 24:00:00 for trips that cross midnight
def service_day_base(yyyymmdd):
    d = datetime.strptime(yyyymmdd, '%Y%m%d')
    return datetime(d.year, d.month, d.day, 12, tzinfo=NY).timestamp() - 12 * 3600

def gtfs_seconds(hhmmss):
    h, m, s = map(int, hhmmss.split(':'))
    return h * 3600 + m * 60 + s

def to_et(ts):
    if ts is None or pd.isna(ts):
        return None
    return datetime.fromtimestamp(ts, tz=NY).strftime('%Y-%m-%d %H:%M:%S')

def to_utc(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if ts else None

# realtime feed
feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(get(RT_URL, 'realtime feed'))

# static schedule, for route/stop names and the scheduled time of every stop
with zipfile.ZipFile(BytesIO(get(STATIC_URL, 'static GTFS'))) as z:
    read = lambda name: pd.read_csv(z.open(name), dtype=str, keep_default_na=False)
    routes = read('routes.txt')
    stops = read('stops.txt')
    trips = read('trips.txt')
    calendar_dates = read('calendar_dates.txt')
    stop_times = read('stop_times.txt')

route_names = dict(zip(routes['route_id'], routes['route_long_name']))
stop_names = dict(zip(stops['stop_id'], stops['stop_name']))
active = calendar_dates[calendar_dates['exception_type'] == '1']
services_by_date = active.groupby('date')['service_id'].apply(set).to_dict()
trips_by_train = {k: g for k, g in trips.groupby('trip_short_name')}
stop_times['stop_sequence'] = stop_times['stop_sequence'].astype(int)
first_departure = stop_times.sort_values('stop_sequence').groupby('trip_id')['departure_time'].first().to_dict()

def match_static_trip(train_number, start_date, start_time):
    # realtime trip_ids don't match the static ones, so a train is found by its
    # number on the service day; a train that leaves after midnight is filed
    # under the previous service day (as 24:xx:xx), so check that day too
    candidates = trips_by_train.get(train_number)
    if candidates is None or not start_date:
        return None
    d = datetime.strptime(start_date, '%Y%m%d')
    rt_start = service_day_base(start_date) + (gtfs_seconds(start_time) if start_time else 0)
    best = None
    for day in (d, d - timedelta(days=1)):
        day_str = day.strftime('%Y%m%d')
        services = services_by_date.get(day_str, set())
        for trip in candidates[candidates['service_id'].isin(services)].itertuples():
            sched_start = service_day_base(day_str) + gtfs_seconds(first_departure[trip.trip_id])
            gap = abs(sched_start - rt_start)
            if best is None or gap < best[0]:
                best = (gap, trip, day_str)
    # without a start_time only a same-day match counts; otherwise starts must line up
    if best is None or best[0] > (12 * 3600 if not start_time else 3 * 3600):
        return None
    return best[1], best[2]

feed_timestamp = feed.header.timestamp
rows = []
matched = unmatched = 0
matched_trip_ids = set()
pending = []
for entity in feed.entity:
    if not entity.HasField('trip_update'):
        continue
    tu = entity.trip_update
    trip = tu.trip
    vehicle = entity.vehicle if entity.HasField('vehicle') else None
    train_number = (vehicle.vehicle.label if vehicle and vehicle.vehicle.label else entity.id)
    match = match_static_trip(train_number, trip.start_date, trip.start_time)
    if match:
        matched += 1
        matched_trip_ids.add(match[0].trip_id)
    else:
        unmatched += 1
    pending.append((entity, tu, trip, vehicle, train_number, match))

# scheduled times for just the matched trips, keyed by (trip_id, stop_id)
sched = stop_times[stop_times['trip_id'].isin(matched_trip_ids)]
sched = sched.drop_duplicates(['trip_id', 'stop_id']).set_index(['trip_id', 'stop_id'])

for entity, tu, trip, vehicle, train_number, match in pending:
    static_trip, service_date = match if match else (None, None)
    base = service_day_base(service_date) if service_date else None
    for stu in tu.stop_time_update:
        ext = mta_extension(stu)
        arrival = stu.arrival.time if stu.HasField('arrival') and stu.arrival.time else None
        departure = stu.departure.time if stu.HasField('departure') and stu.departure.time else None
        s = None
        if static_trip is not None and (static_trip.trip_id, stu.stop_id) in sched.index:
            s = sched.loc[(static_trip.trip_id, stu.stop_id)]
        sched_arrival = base + gtfs_seconds(s['arrival_time']) if s is not None and s['arrival_time'] else None
        sched_departure = base + gtfs_seconds(s['departure_time']) if s is not None and s['departure_time'] else None
        rows.append({
            'train_number': train_number,
            'rt_trip_id': trip.trip_id,
            'static_trip_id': static_trip.trip_id if static_trip is not None else None,
            'route_id': trip.route_id,
            'route_name': route_names.get(trip.route_id),
            'start_date': trip.start_date,
            'start_time': trip.start_time,
            'service_date': service_date,
            'direction_id': static_trip.direction_id if static_trip is not None else None,
            'trip_headsign': static_trip.trip_headsign if static_trip is not None else None,
            'peak_offpeak': static_trip.peak_offpeak if static_trip is not None else None,
            'vehicle_status': gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(vehicle.current_status)
                if vehicle and vehicle.HasField('current_status') else None,
            'vehicle_lat': vehicle.position.latitude if vehicle and vehicle.HasField('position') else None,
            'vehicle_lon': vehicle.position.longitude if vehicle and vehicle.HasField('position') else None,
            'vehicle_timestamp_utc': to_utc(vehicle.timestamp) if vehicle else None,
            'stop_id': stu.stop_id,
            'stop_name': stop_names.get(stu.stop_id),
            'stop_sequence': int(s['stop_sequence']) if s is not None else None,
            'scheduled_arrival_et': to_et(sched_arrival),
            'scheduled_departure_et': to_et(sched_departure),
            'predicted_arrival_et': to_et(arrival),
            'predicted_departure_et': to_et(departure),
            # positive = late; computed against the schedule because MTA only
            # fills its own delay field when a train is off schedule
            'arrival_delay_sec': arrival - sched_arrival if arrival and sched_arrival else None,
            'departure_delay_sec': departure - sched_departure if departure and sched_departure else None,
            'reported_delay_sec': stu.arrival.delay if stu.HasField('arrival') and stu.arrival.HasField('delay') else None,
            'scheduled_track': s['track'] if s is not None and 'track' in s and s['track'] else None,
            'track': ext.get('track'),
            'train_status': ext.get('train_status'),
            'schedule_relationship': gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(stu.schedule_relationship),
        })

# fixed columns, so a file with no trains running still has the full header
COLUMNS = [
    'train_number', 'rt_trip_id', 'static_trip_id', 'route_id', 'route_name',
    'start_date', 'start_time', 'service_date', 'direction_id', 'trip_headsign', 'peak_offpeak',
    'vehicle_status', 'vehicle_lat', 'vehicle_lon', 'vehicle_timestamp_utc',
    'stop_id', 'stop_name', 'stop_sequence',
    'scheduled_arrival_et', 'scheduled_departure_et', 'predicted_arrival_et', 'predicted_departure_et',
    'arrival_delay_sec', 'departure_delay_sec', 'reported_delay_sec',
    'scheduled_track', 'track', 'train_status', 'schedule_relationship',
]
df = pd.DataFrame(rows, columns=COLUMNS)
for col in ['stop_sequence', 'arrival_delay_sec', 'departure_delay_sec', 'reported_delay_sec']:
    df[col] = df[col].astype('Int64')
df['feed_timestamp_utc'] = to_utc(feed_timestamp)
df['datetime'] = RUN_DATETIME
print(f'Feed at {to_utc(feed_timestamp)} UTC: {matched + unmatched} trains '
      f'({unmatched} not matched to the schedule), {len(df)} stop predictions')
if len(df):
    print(df.groupby('route_name', dropna=False)['train_number'].nunique().to_dict())

# a file is written even when no trains are running, so a quiet hour can be
# told apart from a missed run
file_path = 'mta/metro-north-trip-updates/metro-north-trip-updates-' + RUN_LABEL + '.csv'
save_csv(df, file_path)
