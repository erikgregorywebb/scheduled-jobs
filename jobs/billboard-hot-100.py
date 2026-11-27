import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
import boto3
from botocore.exceptions import ClientError
from io import BytesIO
import gzip
import re
import time
import os
import pandas as pd

# Saves the Billboard Hot 100 (https://www.billboard.com/charts/hot-100/) to S3: one row per song
# with rank, title, artist, last week's rank, peak rank, weeks on chart and a new / re-entry flag.
# Billboard publishes the chart on Tuesdays, dated the Saturday of the following week, and the page
# only ever shows the current chart. This runs daily and saves a chart only the first time it sees
# that chart date, so each saved file is one weekly chart. The raw page is saved gzipped beside the
# CSV, so a chart can be re-parsed later if the parser turns out to have missed something.
# robots.txt (checked 2026-11-27) allows /charts/ for User-agent: *.
# There's no API: the chart is parsed from the page's HTML. If Billboard changes the layout,
# the checks below fail the run (and print what they found) rather than saving a bad chart.
#
# PARSE_FILE=page.html python jobs/billboard-hot-100.py parses a saved page instead of fetching
# (and implies DRY_RUN); DRY_RUN=1 fetches and parses without touching S3.

URL = 'https://www.billboard.com/charts/hot-100/'
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; scheduled-jobs data collector)'}
BUCKET = 'egw-data-dumps'
PREFIX = 'billboard-hot-100/'
COLUMNS = ['chart_date', 'rank', 'title', 'artist', 'last_week', 'peak_position',
           'weeks_on_chart', 'status']
MONTHS = 'January|February|March|April|May|June|July|August|September|October|November|December'


def fetch(url):
    # retries 5xx, 429 and connection errors; any other 4xx fails right away
    for attempt in range(4):
        try:
            response = requests.get(url, headers=HEADERS, timeout=60)
            if response.status_code == 429 or response.status_code >= 500:
                raise requests.HTTPError(f'{response.status_code} from {url}', response=response)
            response.raise_for_status()
            return response
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            status = getattr(getattr(e, 'response', None), 'status_code', None)
            if status is not None and 400 <= status < 500 and status != 429:
                raise
            if attempt == 3:
                raise
            print(f'Attempt {attempt + 1} for {url} failed ({e}), retrying')
            time.sleep(15 * (attempt + 1))


def clean(text):
    return re.sub(r'\s+', ' ', text or '').strip()


def chart_date_of(soup, html):
    # the date picker carries the chart date as data-date="YYYY-MM-DD"; fall back to "Week of ..."
    picker = soup.find(id='chart-date-picker')
    if picker is not None and picker.get('data-date'):
        return datetime.strptime(picker['data-date'], '%Y-%m-%d').date(), 'chart-date-picker'
    match = re.search(r'Week of\s+(' + MONTHS + r')\s+(\d{1,2}),\s+(\d{4})', html)
    if match:
        return datetime.strptime(' '.join(match.groups()), '%B %d %Y').date(), '"Week of" text'
    return None, None


def parse_row(row):
    # one <ul class="o-chart-results-list-row"> per song
    title_el = row.find('h3', id='title-of-a-story') or row.find('h3')
    if title_el is None:
        return None
    artist_el = title_el.find_next_sibling('span')
    # every short label in the row: rank first, last week / peak / weeks on chart last,
    # with the NEW or RE-ENTRY badge (if any) in between
    labels = [clean(s.get_text()) for s in row.find_all('span')]
    numbers = [t for t in labels if re.fullmatch(r'\d+|-', t)]
    words = {re.sub(r'[\s-]+', '', t).upper() for t in labels}
    status = 're-entry' if 'REENTRY' in words else 'new' if 'NEW' in words else ''
    if len(numbers) < 4:
        return {'title': clean(title_el.get_text()), 'numbers': numbers}
    return {
        'rank': numbers[0],
        'title': clean(title_el.get_text()),
        'artist': clean(artist_el.get_text()) if artist_el is not None else '',
        'last_week': '' if numbers[-3] == '-' else numbers[-3],
        'peak_position': numbers[-2],
        'weeks_on_chart': numbers[-1],
        'status': status,
        'numbers': numbers,
    }


def describe(soup, rows):
    # what the page looked like, for fixing the parser when a check fails
    lines = [f'  rows (ul.o-chart-results-list-row): {len(rows)}',
             f'  h3#title-of-a-story: {len(soup.find_all("h3", id="title-of-a-story"))}',
             f'  #chart-date-picker: {soup.find(id="chart-date-picker") is not None}']
    first_title = soup.find('h3', id='title-of-a-story')
    if not rows and first_title is not None:
        ancestors = [f'{p.name}.{".".join(p.get("class", []))}' for p in first_title.parents][:6]
        lines.append(f'  first title {clean(first_title.get_text())!r} sits in: {ancestors}')
    for row in rows[:3]:
        lines.append(f'  sample row spans: {[clean(s.get_text()) for s in row.find_all("span")][:20]}')
        lines.append(f'  sample row h3: {[clean(h.get_text()) for h in row.find_all("h3")]}')
    return '\n'.join(lines)


def parse(html):
    soup = BeautifulSoup(html, 'lxml')
    rows = soup.find_all('ul', class_='o-chart-results-list-row')
    chart_date, date_source = chart_date_of(soup, html)
    problems = []
    if chart_date is None:
        problems.append('no chart date found')
    entries = [e for e in (parse_row(r) for r in rows) if e is not None]
    if len(entries) != 100:
        problems.append(f'{len(entries)} songs found, expected 100')
    bad = [e for e in entries if 'rank' not in e]
    if bad:
        problems.append(f'{len(bad)} rows without rank/last week/peak/weeks, e.g. {bad[0]}')
    good = [e for e in entries if 'rank' in e]
    if not bad and [int(e['rank']) for e in good] != list(range(1, len(good) + 1)):
        problems.append(f'ranks are not 1..{len(good)} in order: {[e["rank"] for e in good][:10]}...')
    for e in good:
        rank, peak, weeks = int(e['rank']), int(e['peak_position']), int(e['weeks_on_chart'])
        if not e['title'] or not e['artist']:
            problems.append(f'rank {rank}: missing title or artist ({e})')
        if peak > rank or (e['last_week'] and peak > int(e['last_week'])) or weeks < 1:
            problems.append(f'rank {rank}: inconsistent last week / peak / weeks ({e})')
        if e['last_week'] == '' and weeks > 1 and e['status'] != 're-entry':
            # a song with no last-week rank and more than one week must be re-entering
            problems.append(f'rank {rank}: no last-week rank but {weeks} weeks and no RE-ENTRY badge ({e})')
    if problems:
        raise RuntimeError('Hot 100 page did not parse as expected:\n  - '
                           + '\n  - '.join(problems[:10]) + '\nWhat the page had:\n' + describe(soup, rows))
    df = pd.DataFrame(good)
    df.insert(0, 'chart_date', chart_date.isoformat())
    return df[COLUMNS], chart_date, date_source


parse_file = os.environ.get('PARSE_FILE')
dry_run = bool(os.environ.get('DRY_RUN')) or bool(parse_file)
if parse_file:
    with open(parse_file, encoding='utf-8') as f:
        html = f.read()
else:
    response = fetch(URL)
    if 'text/html' not in response.headers.get('Content-Type', ''):
        raise RuntimeError(f'unexpected Content-Type {response.headers.get("Content-Type")}')
    response.encoding = 'utf-8'
    html = response.text

df, chart_date, date_source = parse(html)
# the chart is dated a Saturday; a stale date means the page stopped updating (or the date is wrong)
if chart_date.weekday() != 5:
    raise RuntimeError(f'chart date {chart_date} (from {date_source}) is not a Saturday')
age = datetime.now(timezone.utc).date() - chart_date
if age > timedelta(days=21) or age < timedelta(days=-14):
    raise RuntimeError(f'chart date {chart_date} (from {date_source}) is {age.days} days from today')

name = 'billboard-hot-100-' + chart_date.isoformat()
csv_key = PREFIX + name + '.csv'
html_key = PREFIX + 'raw/' + name + '.html.gz'
body = df.to_csv(index=False).encode('utf-8')
raw = gzip.compress(html.encode('utf-8'))
summary = (f'chart dated {chart_date}: #1 {df.iloc[0]["title"]} by {df.iloc[0]["artist"]}, '
           f'{(df["status"] == "new").sum()} new, {(df["status"] == "re-entry").sum()} re-entries')

if dry_run:
    print(df.head(10).to_string(index=False))
    print(f'Dry run: would save {summary} to {csv_key} ({len(body):,} bytes) '
          f'and the page to {html_key} ({len(raw):,} bytes)')
else:
    s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    try:
        s3.head_object(Bucket=BUCKET, Key=csv_key)
        print(f'{csv_key} already saved; Billboard has not published a new chart since. Skipping.')
    except ClientError as e:
        if e.response['Error']['Code'] not in ('404', 'NoSuchKey', 'NotFound'):
            raise
        metadata = {'source-url': URL, 'chart-date': chart_date.isoformat(),
                    'downloaded-at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}
        # raw page first, so a saved CSV always has its page beside it
        s3.upload_fileobj(BytesIO(raw), BUCKET, html_key,
                          ExtraArgs={'ContentType': 'text/html', 'ContentEncoding': 'gzip',
                                     'Metadata': metadata})
        s3.upload_fileobj(BytesIO(body), BUCKET, csv_key,
                          ExtraArgs={'ContentType': 'text/csv', 'Metadata': metadata})
        print(f'Copy {summary} to S3 Bucket {BUCKET} at {csv_key}, Done!')
