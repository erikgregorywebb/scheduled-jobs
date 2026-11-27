import requests
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO
import os
import time

# location: Scarsdale, NY, rounded to 2 decimals (~1 km); Open-Meteo's
# finest US model (HRRR) is a ~3 km grid, so extra precision adds nothing
LATITUDE = 40.99
LONGITUDE = -73.80
TIMEZONE = 'America/New_York'

# current-condition variables to collect
VARIABLES = [
    'temperature_2m',
    'relative_humidity_2m',
    'apparent_temperature',
    'dew_point_2m',
    'precipitation',
    'rain',
    'snowfall',
    'weather_code',
    'cloud_cover',
    'pressure_msl',
    'wind_speed_10m',
    'wind_direction_10m',
    'wind_gusts_10m',
    'is_day',
]

# request current conditions, retrying transient failures with backoff
url = 'https://api.open-meteo.com/v1/forecast'
params = {
    'latitude': LATITUDE,
    'longitude': LONGITUDE,
    'timezone': TIMEZONE,
    'current': ','.join(VARIABLES),
    'temperature_unit': 'fahrenheit',
    'wind_speed_unit': 'mph',
    'precipitation_unit': 'inch',
}
RETRY_DELAYS = [10, 30, 60]  # seconds to wait before each retry
for attempt in range(len(RETRY_DELAYS) + 1):
    try:
        response = requests.get(url, params=params, timeout=30)
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        error = e
    else:
        if response.status_code < 500 and response.status_code != 429:
            response.raise_for_status()  # other 4xx means a bad request: fail fast
            data = response.json()
            break
        error = f'HTTP {response.status_code}'
    if attempt == len(RETRY_DELAYS):
        raise RuntimeError(f'Open-Meteo request failed after {attempt + 1} attempts: {error}')
    print(f'Attempt {attempt + 1} failed ({error}), retrying in {RETRY_DELAYS[attempt]}s')
    time.sleep(RETRY_DELAYS[attempt])

# get current datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# one row of current conditions
df = pd.DataFrame([data['current']])

# add request metadata
df['latitude'] = LATITUDE
df['longitude'] = LONGITUDE
df['grid_latitude'] = data.get('latitude')
df['grid_longitude'] = data.get('longitude')
df['grid_elevation_m'] = data.get('elevation')
df['timezone'] = data.get('timezone')
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

file_path = 'weather/hourly/open-meteo-weather-hourly-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath=file_path)
