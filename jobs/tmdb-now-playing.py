import pandas as pd
import os
import time

from common import RUN_DATETIME, RUN_LABEL, get_json, save_csv

# get environmental variables
TMDB_API_KEY = os.environ['TMDB_API_KEY']

# settings
# https://developer.themoviedb.org/reference/movie-now-playing-list
BASE_URL = 'https://api.themoviedb.org/3'
REGION = 'US'
LANGUAGE = 'en-US'
MAX_PAGES = 50  # safety cap; the US list is usually well under this

def tmdb_get(path, params=None):
    params = dict(params or {})
    params['api_key'] = TMDB_API_KEY
    return get_json(BASE_URL + path, params=params, timeout=30, name=path)

# map genre ids to names
genres = tmdb_get('/genre/movie/list', {'language': LANGUAGE})['genres']
genre_lookup = {g['id']: g['name'] for g in genres}

# page through now playing
rows = []
page = 1
total_pages = 1
while page <= total_pages and page <= MAX_PAGES:
    content = tmdb_get('/movie/now_playing', {'region': REGION, 'language': LANGUAGE, 'page': page})
    total_pages = content['total_pages']
    window_min = content.get('dates', {}).get('minimum')
    window_max = content.get('dates', {}).get('maximum')
    for rank, movie in enumerate(content['results'], start=(page - 1) * 20 + 1):
        genre_ids = movie.get('genre_ids', [])
        rows.append([
            movie['id'],
            movie.get('title'),
            movie.get('original_title'),
            movie.get('original_language'),
            movie.get('release_date'),
            '|'.join(genre_lookup.get(g, str(g)) for g in genre_ids),
            movie.get('popularity'),
            movie.get('vote_average'),
            movie.get('vote_count'),
            movie.get('adult'),
            movie.get('overview'),
            movie.get('poster_path'),
            movie.get('backdrop_path'),
            rank,
            page,
            REGION,
            window_min,
            window_max,
            RUN_DATETIME,
        ])
    page += 1
    time.sleep(0.25)

# create dataframe
df = pd.DataFrame(rows, columns=[
    'tmdb_id', 'title', 'original_title', 'original_language', 'release_date',
    'genres', 'popularity', 'vote_average', 'vote_count', 'adult', 'overview',
    'poster_path', 'backdrop_path', 'list_rank', 'page', 'region',
    'window_min', 'window_max', 'datetime',
])
# TMDB pagination can shift mid-crawl and repeat a title across pages
df = df.drop_duplicates(subset='tmdb_id', keep='first')

file_path = 'tmdb-now-playing/tmdb-now-playing-' + RUN_LABEL + '.csv'
save_csv(df, file_path)
