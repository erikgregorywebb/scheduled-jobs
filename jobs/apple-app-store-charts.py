import requests
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO
import os
import time

# get environmental variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# get current date and datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# iPhone charts from Apple's legacy iTunes RSS feed, which supports genre filtering and top grossing
# (the v2 feed ignores genre and has no grossing chart); each feed is capped at 100 apps
charts = ['topfreeapplications', 'toppaidapplications', 'topgrossingapplications']
genres = {'finance': 6015}

def get_json(url, attempts=3):
    for attempt in range(1, attempts + 1):
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                return r.json()
            print(f'Attempt {attempt}: status {r.status_code} for {url}')
        except (requests.RequestException, ValueError) as e:
            print(f'Attempt {attempt}: {e} for {url}')
        time.sleep(5 * attempt)
    return None

def label(entry, key):
    return entry.get(key, {}).get('label')

def get_app_chart(chart, genre, genre_id):
    url = 'https://itunes.apple.com/us/rss/' + chart + '/limit=100/genre=' + str(genre_id) + '/json'
    data = get_json(url)
    if data is None:
        print(f'Skipping {chart} {genre}')
        return None
    feed = data['feed']
    entries = feed.get('entry', [])
    if isinstance(entries, dict):
        entries = [entries]
    rows = []
    for rank, e in enumerate(entries, start=1):
        id_attrs = e['id']['attributes']
        price = e.get('im:price', {}).get('attributes', {})
        cat = e.get('category', {}).get('attributes', {})
        rows.append({
            'rank': rank,
            'app_id': id_attrs.get('im:id'),
            'bundle_id': id_attrs.get('im:bundleId'),
            'app_name': label(e, 'im:name'),
            'artist_name': label(e, 'im:artist'),
            'artist_url': e.get('im:artist', {}).get('attributes', {}).get('href'),
            'price': price.get('amount'),
            'currency': price.get('currency'),
            'app_genre_id': cat.get('im:id'),
            'app_genre': cat.get('label'),
            'release_date': label(e, 'im:releaseDate'),
            'app_url': label(e, 'id'),
            'chart': chart,
            'category': genre,
            'category_genre_id': genre_id,
            'feed_updated': label(feed, 'updated'),
            'url': url,
            'datetime': current_datetime,
        })
    print(f'{chart} {genre}: {len(rows)} apps')
    return pd.DataFrame(rows)

# loop over charts and genres
dfs = []
for chart in charts:
    for genre, genre_id in genres.items():
        df = get_app_chart(chart, genre, genre_id)
        if df is not None and not df.empty:
            dfs.append(df)
        time.sleep(1)
if not dfs:
    print('No data scraped from any chart, exiting.')
    exit(0)
df = pd.concat(dfs, ignore_index=True)

# copy to s3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

file_path = 'apple-app-store-charts/apple-app-store-charts-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath=file_path)
