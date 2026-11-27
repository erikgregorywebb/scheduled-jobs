import requests
import pandas as pd
import time

from common import RUN_DATETIME, RUN_LABEL, get_json, save_csv

# iPhone charts from Apple's legacy iTunes RSS feed, which supports genre filtering and top grossing
# (the v2 feed ignores genre and has no grossing chart); each feed is capped at 100 apps
charts = ['topfreeapplications', 'toppaidapplications', 'topgrossingapplications']
genres = {'finance': 6015}

# fetch a feed, retrying transient failures; None (so the chart is skipped) if it still fails
def fetch_json(url):
    try:
        return get_json(url, delays=(5, 10))
    except (RuntimeError, requests.RequestException, ValueError) as e:
        print(f'{e} for {url}')
        return None

def label(entry, key):
    return entry.get(key, {}).get('label')

def get_app_chart(chart, genre, genre_id):
    url = 'https://itunes.apple.com/us/rss/' + chart + '/limit=100/genre=' + str(genre_id) + '/json'
    data = fetch_json(url)
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
            'datetime': RUN_DATETIME,
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

file_path = 'apple-app-store-charts/apple-app-store-charts-' + RUN_LABEL + '.csv'
save_csv(df, file_path)
