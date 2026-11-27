import requests
import pandas as pd
import time

from common import RUN_DATETIME, RUN_LABEL, get_json, save_csv

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

# fetch a feed, retrying transient failures; None (so the chart is skipped) if it still fails
def fetch_json(url):
    try:
        return get_json(url, delays=(5, 10))
    except (RuntimeError, requests.RequestException, ValueError) as e:
        print(f'{e} for {url}')
        return None

def label(entry, key):
    return entry.get(key, {}).get('label')

def get_show_chart(category, genre_id):
    url = 'https://itunes.apple.com/us/rss/toppodcasts/limit=100/'
    if genre_id is not None:
        url += 'genre=' + str(genre_id) + '/'
    url += 'json'
    data = fetch_json(url)
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
            'datetime': RUN_DATETIME,
        })
    print(f'{category}: {len(rows)} shows')
    return pd.DataFrame(rows)

def get_episode_chart():
    # the legacy feed no longer serves episodes, so use the v2 feed (overall chart only)
    url = 'https://rss.marketingtools.apple.com/api/v2/us/podcasts/top/100/podcast-episodes.json'
    data = fetch_json(url)
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
            'datetime': RUN_DATETIME,
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

if shows is not None:
    file_path = 'apple-podcast-charts/apple-podcast-charts-shows-' + RUN_LABEL + '.csv'
    save_csv(shows, file_path)
else:
    print('No show data scraped from any category.')

if episodes is not None and not episodes.empty:
    file_path = 'apple-podcast-charts/apple-podcast-charts-episodes-' + RUN_LABEL + '.csv'
    save_csv(episodes, file_path)
