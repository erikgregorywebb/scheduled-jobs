import pandas as pd

from common import RUN_DATETIME, RUN_LABEL, get_json, save_csv

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
data = get_json(url, params=params, timeout=30, name='Open-Meteo')

# one row of current conditions
df = pd.DataFrame([data['current']])

# add request metadata
df['latitude'] = LATITUDE
df['longitude'] = LONGITUDE
df['grid_latitude'] = data.get('latitude')
df['grid_longitude'] = data.get('longitude')
df['grid_elevation_m'] = data.get('elevation')
df['timezone'] = data.get('timezone')
df['datetime'] = RUN_DATETIME

file_path = 'weather/hourly/open-meteo-weather-hourly-' + RUN_LABEL + '.csv'
save_csv(df, file_path)
