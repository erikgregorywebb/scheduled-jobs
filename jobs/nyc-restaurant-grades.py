import pandas as pd
import os

from common import RUN_DATETIME, RUN_LABEL, get_json, save_csv

# get environmental variables
# optional: without it Socrata still answers, but with a lower shared rate limit
NYC_OPEN_DATA_APP_TOKEN = os.environ.get('NYC_OPEN_DATA_APP_TOKEN')

# settings
# DOHMH New York City Restaurant Inspection Results: one row per violation cited,
# covering every active restaurant's inspections from the last ~3 years.
# The dataset is rebuilt daily, and restaurants that close drop out of it.
URL = 'https://data.cityofnewyork.us/resource/43nn-pn8j.json'
PREFIX = 'nyc-restaurant-grades/'
PAGE_SIZE = 50000
# ~31.5k restaurants and ~55k graded inspections in Nov 2026; far fewer means a bad response
MIN_RESTAURANTS = 20000
MIN_GRADED_INSPECTIONS = 30000

RESTAURANT_COLS = [
    'camis', 'dba', 'boro', 'building', 'street', 'zipcode', 'phone', 'cuisine_description',
    'latitude', 'longitude', 'community_board', 'council_district', 'census_tract', 'bin', 'bbl', 'nta',
]
INSPECTION_COLS = ['camis', 'inspection_date', 'inspection_type', 'action', 'score', 'grade', 'grade_date']

headers = {'X-App-Token': NYC_OPEN_DATA_APP_TOKEN} if NYC_OPEN_DATA_APP_TOKEN else {}

def get_page(params):
    # one request, retrying transient failures and fast-failing other 4xx
    data = get_json(URL, params=params, headers=headers, timeout=120, delays=(20, 40), name='restaurant inspections')
    if not isinstance(data, list):
        raise SystemExit(f'Expected a list of rows, got: {str(data)[:200]}')
    return data

def get_all(params):
    # page through a grouped query; $order keeps paging stable
    rows = []
    while True:
        page = get_page({**params, '$limit': PAGE_SIZE, '$offset': len(rows)})
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            return rows

# restaurants: one row per CAMIS (restaurant ID). Location fields are the same on every
# row for a restaurant, so grouping by them collapses the violations to one row each.
restaurants = pd.DataFrame(get_all({
    '$select': ', '.join(RESTAURANT_COLS) + ', max(inspection_date) as last_inspection_date, max(record_date) as record_date',
    '$group': ', '.join(RESTAURANT_COLS),
    '$order': 'camis',
}))
if len(restaurants) < MIN_RESTAURANTS:
    raise SystemExit(f'Got only {len(restaurants)} restaurants; expected at least {MIN_RESTAURANTS}')
if restaurants['camis'].duplicated().any():
    raise SystemExit(f"{restaurants['camis'].duplicated().sum()} restaurants have more than one name/location row")

# graded inspections: one row per inspection that carried a grade (A, B, C, N, P, Z)
inspections = pd.DataFrame(get_all({
    '$select': ', '.join(INSPECTION_COLS),
    '$where': 'grade IS NOT NULL',
    '$group': ', '.join(INSPECTION_COLS),
    '$order': ', '.join(INSPECTION_COLS),
}))
if len(inspections) < MIN_GRADED_INSPECTIONS:
    raise SystemExit(f'Got only {len(inspections)} graded inspections; expected at least {MIN_GRADED_INSPECTIONS}')

# N grades have no grade_date, so order by the inspection date where it's missing.
# The same visit is sometimes logged under two inspection types with the same grade;
# keep the one with a score.
inspections['grade_sort_date'] = inspections['grade_date'].fillna(inspections['inspection_date'])
inspections['has_score'] = inspections['score'].notna()
inspections = (
    inspections
    .sort_values(['camis', 'grade_sort_date', 'inspection_date', 'has_score'], ascending=[True, False, False, False])
    .drop_duplicates(['camis', 'grade_sort_date'])
)
inspections['grade_rank'] = inspections.groupby('camis').cumcount()
n_graded = inspections.groupby('camis').size().rename('n_graded_inspections')

def nth_grade(rank, prefix):
    cols = ['inspection_date', 'inspection_type', 'score', 'grade', 'grade_date']
    df = inspections.loc[inspections['grade_rank'] == rank, ['camis'] + cols]
    return df.rename(columns={c: prefix + c for c in cols})

df = (
    restaurants
    .merge(nth_grade(0, 'current_'), on='camis', how='left')
    .merge(nth_grade(1, 'previous_'), on='camis', how='left')
    .merge(n_graded, on='camis', how='left')
)
df['n_graded_inspections'] = df['n_graded_inspections'].fillna(0).astype(int)
# Socrata timestamps are NYC local time with no zone; keep just the date
for col in [c for c in df.columns if c.endswith('_date')]:
    df[col] = df[col].str[:10]
# 1900-01-01 marks a new restaurant that hasn't been inspected yet
df.loc[df['last_inspection_date'] == '1900-01-01', 'last_inspection_date'] = None
df['scraped_at'] = RUN_DATETIME

print(f"{len(df)} restaurants, {df['current_grade'].notna().sum()} with a grade:")
print(df['current_grade'].value_counts(dropna=False).to_string())

# copy to s3
file_path = PREFIX + 'nyc-restaurant-grades-' + RUN_LABEL + '.csv'
save_csv(df, file_path)
