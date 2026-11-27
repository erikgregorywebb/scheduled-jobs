import requests
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import zipfile
import boto3
from io import BytesIO, StringIO
import json
import time
import os

# Snapshots Metro-North's GTFS-realtime service alerts: live delay and
# cancellation notices for individual trains (usually with the cause, e.g. "a
# fallen tree north of Patterson"), plus planned work, schedule changes and
# station notices. Pairs with metro-north-trip-updates.py: alerts about a
# specific train carry its train number and static trip_id, which match the
# train_number and static_trip_id columns there.
# The feed is overwritten continuously, so these hourly snapshots are the
# history. No API key needed (checked 2026-11-27).
# The .json form of the feed decodes MTA's "Mercury" extension (alert type,
# created/updated times, a plain-English active period), which the public
# GTFS-realtime bindings don't define.

ALERTS_URL = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fmnr-alerts.json'
STATIC_URL = 'https://rrgtfsfeeds.s3.amazonaws.com/gtfsmnr.zip'
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; scheduled-jobs data collector)'}
NY = ZoneInfo('America/New_York')
MERCURY = 'transit_realtime.mercury_alert'

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

def to_et(ts):
    return datetime.fromtimestamp(ts, tz=NY).strftime('%Y-%m-%d %H:%M:%S') if ts else None

def to_utc(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if ts else None

def text(translated, language='en'):
    # a TranslatedString: pick the plain-English translation ('en-html' is also sent)
    for t in (translated or {}).get('translation', []):
        if t.get('language') == language:
            return t.get('text')
    return None

def joined(values):
    # unique values in first-seen order, ';'-separated
    out = list(dict.fromkeys(v for v in values if v))
    return ';'.join(out) if out else None

feed = json.loads(get(ALERTS_URL, 'alerts feed'))

# static schedule, for route/stop names and train numbers
with zipfile.ZipFile(BytesIO(get(STATIC_URL, 'static GTFS'))) as z:
    read = lambda name: pd.read_csv(z.open(name), dtype=str, keep_default_na=False)
    routes = read('routes.txt')
    stops = read('stops.txt')
    trips = read('trips.txt')
# alerts also use route_ids the static GTFS doesn't have: the Wassaic branch
# of the Harlem Line, and the West of Hudson lines NJ Transit runs for
# Metro-North (ids confirmed from alert text, 2026-11-27)
EXTRA_ROUTE_NAMES = {'wassaic': 'Harlem (Wassaic)', '50': 'Pascack Valley', '51': 'Port Jervis'}
route_names = {**EXTRA_ROUTE_NAMES, **dict(zip(routes['route_id'], routes['route_long_name']))}
stop_names = dict(zip(stops['stop_id'], stops['stop_name']))
train_numbers = dict(zip(trips['trip_id'], trips['trip_short_name']))

feed_timestamp = int(feed['header']['timestamp'])
rows = []
for entity in feed.get('entity', []):
    alert = entity.get('alert')
    if not alert:
        continue
    mercury = alert.get(MERCURY, {})
    periods = alert.get('active_period', [])
    starts = [int(p['start']) for p in periods if p.get('start')]
    ends = [int(p['end']) for p in periods if p.get('end')]
    # a period with no start began in the past; one with no end is open-ended
    active_now = any(int(p.get('start') or 0) <= feed_timestamp < int(p.get('end') or 2**63) for p in periods)
    entities = alert.get('informed_entity', [])
    trips_informed = [e['trip'] for e in entities if e.get('trip')]
    route_ids = [e.get('route_id') for e in entities] + [t.get('route_id') for t in trips_informed]
    stop_ids = [e.get('stop_id') for e in entities]
    trip_ids = [t.get('trip_id') for t in trips_informed]
    rows.append({
        'alert_id': entity.get('id'),
        # ids look like 'lmm:alert:278160:24' (live) or 'lmm:planned_work:36815'
        'alert_source': entity.get('id', '').split(':')[1] if entity.get('id', '').count(':') >= 1 else None,
        'alert_type': mercury.get('alert_type'),
        'header_text': text(alert.get('header_text')),
        'description_text': text(alert.get('description_text')),
        'active_period_text': text(mercury.get('human_readable_active_period')),
        'active_now': active_now,
        'first_start_et': to_et(min(starts)) if starts else None,
        'last_end_et': to_et(max(ends)) if len(ends) == len(periods) and ends else None,
        'num_active_periods': len(periods),
        'created_at_et': to_et(mercury.get('created_at')),
        'updated_at_et': to_et(mercury.get('updated_at')),
        'display_before_active_sec': mercury.get('display_before_active'),
        # an unknown route_id keeps its raw id as the name
        'route_ids': joined(route_ids),
        'route_names': joined(route_names.get(r, r) for r in route_ids if r),
        'stop_ids': joined(stop_ids),
        'stop_names': joined(stop_names.get(s, s) for s in stop_ids if s),
        'train_numbers': joined(train_numbers.get(t) for t in trip_ids),
        'trip_ids': joined(trip_ids),
        'cause': alert.get('cause'),
        'effect': alert.get('effect'),
        # full detail, kept whole so nothing is lost by the flattening above
        'active_periods': json.dumps(periods),
        'informed_entities': json.dumps(entities),
    })

COLUMNS = [
    'alert_id', 'alert_source', 'alert_type', 'header_text', 'description_text',
    'active_period_text', 'active_now', 'first_start_et', 'last_end_et', 'num_active_periods',
    'created_at_et', 'updated_at_et', 'display_before_active_sec',
    'route_ids', 'route_names', 'stop_ids', 'stop_names', 'train_numbers', 'trip_ids',
    'cause', 'effect', 'active_periods', 'informed_entities',
]
df = pd.DataFrame(rows, columns=COLUMNS)
df['display_before_active_sec'] = df['display_before_active_sec'].astype('Int64')
df['feed_timestamp_utc'] = to_utc(feed_timestamp)

current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
df['datetime'] = current_datetime
print(f'Feed at {to_utc(feed_timestamp)} UTC: {len(df)} alerts ({int(df["active_now"].sum())} active now)',
      df['alert_type'].value_counts().to_dict() if len(df) else '')

if os.environ.get('DRY_RUN'):
    # local check without touching S3: DRY_RUN=1 python jobs/metro-north-service-alerts.py
    df.to_csv('metro-north-service-alerts.csv', index=False)
    print('Dry run: wrote metro-north-service-alerts.csv')
else:
    # get environmental variables
    AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

    # copy to s3; a file is written even with no alerts, so a quiet hour can be
    # told apart from a missed run
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    def copy_to_s3(client, df, bucket, filepath):
        csv_buf = StringIO()
        df.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)
        client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
        print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

    file_path = 'mta/metro-north-service-alerts/metro-north-service-alerts-' + current_datetime_label + '.csv'
    copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath=file_path)
