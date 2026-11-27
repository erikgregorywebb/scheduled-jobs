from io import StringIO

import pandas as pd

from common import RUN_DATETIME, RUN_LABEL, get, save_csv

# TSA publishes daily checkpoint passenger counts as an HTML table: a Date
# column, then one column per year shown (currently a single "Numbers" column)
URL = 'https://www.tsa.gov/travel/passenger-volumes'

page = get(URL, timeout=60, name='tsa').text
tables = pd.read_html(StringIO(page), thousands=None)
if not tables:
    raise RuntimeError('No table found on the TSA passenger volumes page; the page may have changed')
table = tables[0].astype(str)
if 'Date' not in table.columns or len(table.columns) < 2 or len(table) == 0:
    raise RuntimeError(f'Unexpected TSA table shape: columns {list(table.columns)}, {len(table)} rows')

# one row per date and year column
df = table.melt(id_vars='Date', var_name='year', value_name='passenger_volume').rename(columns={'Date': 'date'})
df['passenger_volume'] = pd.to_numeric(df['passenger_volume'].str.replace(',', '', regex=False), errors='coerce').astype('Int64')
if df['passenger_volume'].notna().sum() == 0:
    raise RuntimeError('TSA table has no numeric passenger volumes')
df['scraped_at'] = RUN_DATETIME

save_csv(df, 'tsa/tsa-passenger-volumes-' + RUN_LABEL + '.csv')
