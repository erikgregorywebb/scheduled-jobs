# https://www.pythonanywhere.com/user/YFmKUEkNRTZFrf/ipython_notebooks/view/kworb.ipynb

import pandas as pd
from io import BytesIO

from common import RUN_LABEL, get, save_csv

# fetch a page (with a timeout and retries) and parse its tables
def read_html(url):
    return pd.read_html(BytesIO(get(url).content))

# us daily
url = 'https://kworb.net/spotify/country/us_daily.html'
raw = read_html(url)
df_us_daily = raw[0]

# us daily totals
url = 'https://kworb.net/spotify/country/us_daily_totals.html'
raw = read_html(url)
df_us_daily_totals = raw[0]

# artists
url = 'https://kworb.net/spotify/artists.html'
raw = read_html(url)
raw_0 = raw[0]
new_header = raw_0.iloc[0]
raw_0 = raw_0[1:]
raw_0.columns = new_header
df_artists = raw_0

# listeners
url = 'https://kworb.net/spotify/listeners.html'
raw = read_html(url)
df_listeners = raw[0]

file_path = 'kworb/us-daily-' + RUN_LABEL + '.csv'
save_csv(df_us_daily, file_path)

file_path = 'kworb/us-daily-totals-' + RUN_LABEL + '.csv'
save_csv(df_us_daily_totals, file_path)

file_path = 'kworb/artists-' + RUN_LABEL + '.csv'
save_csv(df_artists, file_path)

file_path = 'kworb/listeners-' + RUN_LABEL + '.csv'
save_csv(df_listeners, file_path)
