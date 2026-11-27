import pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo

from common import RUN_DATETIME, RUN_TIME, get_json, save_csv

# daily backfill for open-meteo-weather-hourly.py: saves yesterday's 24 hourly
# readings so hours missed by the hourly job (failed or skipped runs) can be filled

# location: Scarsdale, NY, rounded to 2 decimals (~1 km), same as the hourly job
LATITUDE = 40.99
LONGITUDE = -73.80
TIMEZONE = 'America/New_York'

# same variables as the hourly job
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

# yesterday, in local time
yesterday = (RUN_TIME.astimezone(ZoneInfo(TIMEZONE)) - timedelta(days=1)).date().isoformat()

# request yesterday's hourly values, retrying transient failures with backoff
url = 'https://api.open-meteo.com/v1/forecast'
params = {
    'latitude': LATITUDE,
    'longitude': LONGITUDE,
    'timezone': TIMEZONE,
    'hourly': ','.join(VARIABLES),
    'start_date': yesterday,
    'end_date': yesterday,
    'temperature_unit': 'fahrenheit',
    'wind_speed_unit': 'mph',
    'precipitation_unit': 'inch',
}
data = get_json(url, params=params, timeout=30, name='Open-Meteo')

# one row per hour of yesterday
df = pd.DataFrame(data['hourly'])

# add request metadata
df['latitude'] = LATITUDE
df['longitude'] = LONGITUDE
df['grid_latitude'] = data.get('latitude')
df['grid_longitude'] = data.get('longitude')
df['grid_elevation_m'] = data.get('elevation')
df['timezone'] = data.get('timezone')
df['datetime'] = RUN_DATETIME

# named by the date the data covers, so a rerun overwrites rather than duplicates
file_path = 'weather/daily/open-meteo-weather-daily-' + yesterday + '.csv'
save_csv(df, file_path)
