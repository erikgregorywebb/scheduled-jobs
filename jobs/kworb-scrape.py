# https://www.pythonanywhere.com/user/YFmKUEkNRTZFrf/ipython_notebooks/view/kworb.ipynb

import requests
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO
import os

# get environmental variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# get current date and datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# us daily
url = 'https://kworb.net/spotify/country/us_daily.html'
raw = pd.read_html(url)
df_us_daily = raw[0]

# us daily totals
url = 'https://kworb.net/spotify/country/us_daily_totals.html'
raw = pd.read_html(url)
df_us_daily_totals = raw[0]

# artists
url = 'https://kworb.net/spotify/artists.html'
raw = pd.read_html(url)
raw_0 = raw[0]
new_header = raw_0.iloc[0]
raw_0 = raw_0[1:]
raw_0.columns = new_header
df_artists = raw_0

# listeners
url = 'https://kworb.net/spotify/listeners.html'
raw = pd.read_html(url)
df_listeners = raw[0]

# copy to s3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

file_path = 'kworb/us-daily-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df_us_daily, bucket='egw-data-dumps', filepath=file_path)

file_path = 'kworb/us-daily-totals-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df_us_daily_totals, bucket='egw-data-dumps', filepath=file_path)

file_path = 'kworb/artists-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df_artists, bucket='egw-data-dumps', filepath=file_path)

file_path = 'kworb/listeners-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df_listeners, bucket='egw-data-dumps', filepath=file_path)
