import requests
import pandas as pd
from datetime import datetime, timezone
import boto3
from io import StringIO
import os
import time

# Citi Bike's public GBFS feeds (operated by Lyft); refreshed every ~60s
BASE_URL = 'https://gbfs.lyft.com/gbfs/2.3/bkn/en/'

# vehicle_type_id values from the vehicle_types feed
CLASSIC_TYPE_ID = '1'  # human-powered bicycle
EBIKE_TYPE_ID = '2'    # electric-assist bicycle

# fetch a GBFS feed, retrying transient failures with backoff
RETRY_DELAYS = [10, 30, 60]  # seconds to wait before each retry
def get_feed(name):
    url = BASE_URL + name + '.json'
    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            response = requests.get(url, timeout=30)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            error = e
        else:
            if response.status_code < 500 and response.status_code != 429:
                response.raise_for_status()  # other 4xx means a bad request: fail fast
                return response.json()
            error = f'HTTP {response.status_code}'
        if attempt == len(RETRY_DELAYS):
            raise RuntimeError(f'{name} request failed after {attempt + 1} attempts: {error}')
        print(f'{name} attempt {attempt + 1} failed ({error}), retrying in {RETRY_DELAYS[attempt]}s')
        time.sleep(RETRY_DELAYS[attempt])

status = get_feed('station_status')
info = get_feed('station_information')

# get current datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# one row per station, with classic/e-bike counts split out of vehicle_types_available
rows = []
for s in status['data']['stations']:
    counts = {v['vehicle_type_id']: v['count'] for v in s.get('vehicle_types_available', [])}
    rows.append({
        'station_id': s['station_id'],
        'num_bikes_available': s.get('num_bikes_available'),
        'num_classic_available': counts.get(CLASSIC_TYPE_ID),
        'num_ebikes_available': counts.get(EBIKE_TYPE_ID, s.get('num_ebikes_available')),
        'num_bikes_disabled': s.get('num_bikes_disabled'),
        'num_docks_available': s.get('num_docks_available'),
        'num_docks_disabled': s.get('num_docks_disabled'),
        'is_installed': s.get('is_installed'),
        'is_renting': s.get('is_renting'),
        'is_returning': s.get('is_returning'),
        'last_reported': s.get('last_reported'),
    })
df = pd.DataFrame(rows)

# convert station report times (unix seconds) to UTC timestamps; decommissioned
# stations report placeholder values near the epoch, which are left as-is
df['last_reported_utc'] = pd.to_datetime(df['last_reported'], unit='s', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')

# attach station name, location and capacity so each file stands on its own
stations = pd.DataFrame(info['data']['stations'])
stations = stations[['station_id', 'name', 'short_name', 'lat', 'lon', 'region_id', 'capacity']]
df = df.merge(stations, on='station_id', how='left')

# add feed metadata
df['feed_last_updated_utc'] = datetime.fromtimestamp(status['last_updated'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
df['datetime'] = current_datetime

# get environmental variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# copy to s3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

file_path = 'citibike/station-status/citibike-station-status-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath=file_path)
