import pandas as pd
from datetime import datetime, timezone

from common import RUN_DATETIME, RUN_LABEL, get_json, save_csv

# Citi Bike's public GBFS feeds (operated by Lyft); refreshed every ~60s
BASE_URL = 'https://gbfs.lyft.com/gbfs/2.3/bkn/en/'

# vehicle_type_id values from the vehicle_types feed
CLASSIC_TYPE_ID = '1'  # human-powered bicycle
EBIKE_TYPE_ID = '2'    # electric-assist bicycle

def get_feed(name):
    return get_json(BASE_URL + name + '.json', name=name)

status = get_feed('station_status')
info = get_feed('station_information')

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
df['datetime'] = RUN_DATETIME

file_path = 'citibike/station-status/citibike-station-status-' + RUN_LABEL + '.csv'
save_csv(df, file_path)
