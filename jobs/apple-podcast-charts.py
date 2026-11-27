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

# Apple Podcasts top-level genre ids; None is the overall chart
# the legacy iTunes RSS feed is used because it supports genre filtering (the v2 feed ignores it)
categories = {
    'top': None,
    'arts': 1301,
    'business': 1321,
    'comedy': 1303,
    'education': 1304,
    'fiction': 1483,
    'health & fitness': 1512,
    'history': 1487,
    'kids & family': 1305,
    'leisure': 1502,
    'music': 1310,
    'news': 1489,
    'religion & spirituality': 1314,
    'science': 1533,
    'society & culture': 1324,
    'sports': 1545,
    'technology': 1318,
    'true crime': 1488,
    'tv & film': 1309,
}

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

def get_show_chart(category, genre_id):
    url = 'https://itunes.apple.com/us/rss/toppodcasts/limit=100/'
    if genre_id is not None:
        url += 'genre=' + str(genre_id) + '/'
    url += 'json'
    data = get_json(url)
    if data is None:
        print(f'Skipping category {category}')
        return None
    feed = data['feed']
    entries = feed.get('entry', [])
    if isinstance(entries, dict):
        entries = [entries]
    rows = []
    for rank, e in enumerate(entries, start=1):
        cat = e.get('category', {}).get('attributes', {})
        rows.append({
            'rank': rank,
            'show_id': e['id']['attributes']['im:id'],
            'show_name': label(e, 'im:name'),
            'artist_name': label(e, 'im:artist'),
            'show_genre_id': cat.get('im:id'),
            'show_genre': cat.get('label'),
            'release_date': label(e, 'im:releaseDate'),
            'show_url': label(e, 'id'),
            'summary': label(e, 'summary'),
            'chart': 'top-shows',
            'category': category,
            'category_genre_id': genre_id,
            'feed_updated': label(feed, 'updated'),
            'url': url,
            'datetime': current_datetime,
        })
    print(f'{category}: {len(rows)} shows')
    return pd.DataFrame(rows)

def get_episode_chart():
    # the legacy feed no longer serves episodes, so use the v2 feed (overall chart only)
    url = 'https://rss.marketingtools.apple.com/api/v2/us/podcasts/top/100/podcast-episodes.json'
    data = get_json(url)
    if data is None:
        print('Skipping episode chart')
        return None
    feed = data['feed']
    rows = []
    for rank, r in enumerate(feed.get('results', []), start=1):
        genres = r.get('genres') or []
        rows.append({
            'rank': rank,
            'episode_id': r.get('id'),
            'episode_name': r.get('name'),
            'artist_name': r.get('artistName'),
            'genre': genres[0].get('name') if genres else None,
            'episode_url': r.get('url'),
            'chart': 'top-episodes',
            'feed_updated': feed.get('updated'),
            'url': url,
            'datetime': current_datetime,
        })
    print(f'episodes: {len(rows)} episodes')
    return pd.DataFrame(rows)

# loop over categories
dfs = []
for category, genre_id in categories.items():
    df = get_show_chart(category, genre_id)
    if df is not None and not df.empty:
        dfs.append(df)
    time.sleep(1)
shows = pd.concat(dfs, ignore_index=True) if dfs else None
episodes = get_episode_chart()

# copy to s3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

if shows is not None:
    file_path = 'apple-podcast-charts/apple-podcast-charts-shows-' + current_datetime_label + '.csv'
    copy_to_s3(client=s3, df=shows, bucket='egw-data-dumps', filepath=file_path)
else:
    print('No show data scraped from any category.')

if episodes is not None and not episodes.empty:
    file_path = 'apple-podcast-charts/apple-podcast-charts-episodes-' + current_datetime_label + '.csv'
    copy_to_s3(client=s3, df=episodes, bucket='egw-data-dumps', filepath=file_path)
