import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import boto3
from io import StringIO
import json
import os
import re
import time

# get environmental variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# get current date and datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# settings
# Ski Utah's snow report lists every Utah resort; each links to a resort page,
# and each resort page loads its charts from <resort page>/../graphdata (JSON)
OVERVIEW_URL = 'https://www.skiutah.com/snowreport'
RESORT_URL = 'https://www.skiutah.com/members/{slug}/snowreport'
GRAPH_URL = 'https://www.skiutah.com/members/{slug}/graphdata'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}
BUCKET = 'egw-data-dumps'
PREFIX = 'ski-utah-snow-reports/'
# resorts in the Wasatch Range (the rest are in the Bear River Range, Tushars,
# Wasatch Plateau, and near Cedar City); every resort is collected, this just flags them
WASATCH = {
    'alta', 'snowbird', 'brighton', 'solitude', 'park-city-mountain', 'deer-valley',
    'woodward-park-city', 'snowbasin', 'powder-mountain', 'nordic-valley', 'sundance',
}
# Ski Utah lists 16 resorts as of the 2026-27 season; far fewer means a broken page
MIN_RESORTS = 12

def get(url, expect_json=False):
    # retry transient failures, fail fast on other 4xx
    for attempt in range(1, 4):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code == 200:
                return r.json() if expect_json else r.text
            if r.status_code < 500 and r.status_code != 429:
                raise SystemExit(f'{url} failed with status {r.status_code}: {r.text[:200]}')
            print(f'{url} attempt {attempt}: status {r.status_code}')
        except (requests.RequestException, ValueError) as e:
            print(f'{url} attempt {attempt}: {e}')
        time.sleep(10 * attempt)
    raise SystemExit(f'{url} failed after 3 attempts')

def clean(text):
    return re.sub(r'\s+', ' ', text).strip() if text else None

def number(text):
    # '36”' -> 36, '9,664’' -> 9664, '--' or '' -> None
    if not text:
        return None
    m = re.search(r'-?\d[\d,]*(\.\d+)?', text)
    return float(m.group(0).replace(',', '')) if m else None

# 1. the resort list, with the status shown on the overview (e.g. 'Opening Dec 4')
overview = BeautifulSoup(get(OVERVIEW_URL), 'lxml')
resorts = {}
for a in overview.find_all('a', href=re.compile(r'/members/[^/]+/snowreport$')):
    slug = a['href'].rstrip('/').split('/')[-2]
    if slug in resorts:
        continue
    row = a.find_parent('li')
    override = row.find(class_='override') if row else None
    resorts[slug] = {'short_name': clean(a.get_text()), 'overview_status': clean(override.get_text()) if override else None}
if len(resorts) < MIN_RESORTS:
    raise SystemExit(f'Found only {len(resorts)} resorts on {OVERVIEW_URL}; expected at least {MIN_RESORTS}')
print(f'Found {len(resorts)} resorts')

# 2. each resort's page and chart data
report_rows = []
season_rows = []
for slug, info in resorts.items():
    url = RESORT_URL.format(slug=slug)
    soup = BeautifulSoup(get(url), 'lxml')
    page = soup.find(id='page-info')
    if page is None or page.find('h1') is None:
        raise SystemExit(f'{url} has no snow report; got title {soup.title.string if soup.title else None!r}')

    # the labelled values: Overnight, Past 24 HR, Past 48 HR, base depth, elevations, ...
    # (only the top of the page; the weather forecast further down reuses the same classes)
    top = page.find(class_='snowreport-resort-top-page')
    values = {}
    for dt in top.find_all('dt', class_='snowreport-forecast-title'):
        dd = dt.find_next_sibling('dd')
        # 'Utah Powder Days This Season i' -> drop the trailing info-popover 'i'
        values[re.sub(r' i$', '', clean(dt.get_text(' ')).lower())] = clean(dd.get_text()) if dd else None

    # runs / lifts / parks open, e.g. {"value": "72/118"}
    ops = {}
    for clock in top.find_all(attrs={'data-progress-clock': True}):
        kind = clock.get('aria-label', '').replace('amount of ', '').replace(' open', '')
        value = json.loads(clock['data-progress-clock']).get('value', '')
        m = re.match(r'\s*(\d+)\s*/\s*(\d+)', value)
        ops[kind] = (int(m.group(1)), int(m.group(2))) if m else (None, None)

    updated = top.find(class_='snowreport-updated')
    updated = updated.find('span').get_text(strip=True) if updated and updated.find('span') else None
    hours = '; '.join(
        clean(i.get_text(' ')) for i in top.find_all(class_='service-hours-item')
    ) or None
    opening = top.find(class_='snowreport-open')
    comments = top.find(class_='top-page-comment')
    comments = clean(comments.get_text(' ').split('comments:', 1)[-1]) if comments else None
    last7 = re.search(r'last 7 days\s*([\d.]+)', clean(top.get_text(' ')) or '')

    report_rows.append({
        'slug': slug,
        'resort': clean(page.find('h1').get_text()),
        'short_name': info['short_name'],
        'wasatch': slug in WASATCH,
        'overview_status': info['overview_status'],
        'opening_status': clean(opening.get_text(' ')) if opening else None,
        'hours': hours,
        'report_updated_utc': updated,
        'overnight_in': number(values.get('overnight')),
        'snow_24h_in': number(values.get('past 24 hr')),
        'snow_48h_in': number(values.get('past 48 hr')),
        'snow_7d_in': float(last7.group(1)) if last7 else None,
        'base_depth_in': number(values.get('base depth')),
        'season_total_in': number(values.get('year to date')),
        'powder_days_season': number(values.get('utah powder days this season')),
        'runs_open': ops.get('terrain', (None, None))[0],
        'runs_total': ops.get('terrain', (None, None))[1],
        'lifts_open': ops.get('lift', (None, None))[0],
        'lifts_total': ops.get('lift', (None, None))[1],
        'parks_open': ops.get('park', (None, None))[0],
        'parks_total': ops.get('park', (None, None))[1],
        'snow_stake_elev_ft': number(values.get('snow stake elev.')),
        'base_elev_ft': number(values.get('base elev.')),
        'peak_elev_ft': number(values.get('peak elev.')),
        'comments': comments if comments not in ('', '--') else None,
        'url': url,
        'datetime': current_datetime,
    })

    # this season's daily base depth and year-to-date snowfall, as charted on the page
    graph = get(GRAPH_URL.format(slug=slug), expect_json=True).get('graphdata', {})
    season = graph.get('snowfall_season') or {}
    dates = (season.get('xAxis') or {}).get('categories') or []
    series = {s.get('name'): s.get('data') or [] for s in season.get('series') or []}
    for i, d in enumerate(dates):
        ytd = series.get('YTD', [])
        base = series.get('Base', [])
        season_rows.append({
            'slug': slug,
            'date': d,
            'base_depth_in': base[i] if i < len(base) else None,
            'season_total_in': ytd[i] if i < len(ytd) else None,
            'datetime': current_datetime,
        })
    time.sleep(1)  # be polite: ~32 requests per run

report = pd.DataFrame(report_rows)
season = pd.DataFrame(season_rows, columns=['slug', 'date', 'base_depth_in', 'season_total_in', 'datetime'])

# a page that parsed but lost every number is a layout change, not a quiet day
if report[['base_depth_in', 'runs_total']].isna().all().all():
    raise SystemExit('Every resort is missing base depth and run counts; the page layout may have changed')

for r in report.itertuples():
    print(f'{r.short_name:<20} 24h {r.snow_24h_in}  base {r.base_depth_in}  season {r.season_total_in}  runs {r.runs_open}/{r.runs_total}  {r.overview_status or r.hours or ""}')

# copy to s3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

copy_to_s3(client=s3, df=report, bucket=BUCKET, filepath=PREFIX + 'ski-utah-snow-reports-' + current_datetime_label + '.csv')
copy_to_s3(client=s3, df=season, bucket=BUCKET, filepath=PREFIX + 'ski-utah-snow-season-' + current_datetime_label + '.csv')
