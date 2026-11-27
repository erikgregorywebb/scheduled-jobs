import requests
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
import boto3
from botocore.exceptions import ClientError
from io import BytesIO
import gzip
import time
import os

# Saves Netflix's official Top 10 downloads (https://www.netflix.com/tudum/top10,
# formerly top10.netflix.com, which now redirects there) to S3, gzipped but otherwise unchanged.
#   all-weeks-global.tsv     every week since 2021-07-04: top 10 films/TV, English and non-English,
#                            with hours viewed, runtime, views and weeks in the top 10
#   all-weeks-countries.tsv  every week since 2021-07-04: top 10 films and TV in each of ~95 countries
#   most-popular.tsv         the all-time most popular titles by views in their first 91 days;
#                            overwritten in place, so these copies are its only history
# Netflix publishes once a week, on Tuesday afternoons (US time), for the week ending the
# previous Sunday, and sometimes revises earlier weeks. This runs daily and saves a file
# only when Netflix's Last-Modified changes, so each saved copy is one release.
# robots.txt (checked 2026-11-27) disallows / for unlisted user agents but allows /tudum for
# several named ones; these files are published as public downloads linked from the Top 10 page.

BASE_URL = 'https://www.netflix.com/tudum/top10/data/'
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; scheduled-jobs data collector)'}
BUCKET = 'egw-data-dumps'
PREFIX = 'netflix-top-10/'
CATEGORIES_GLOBAL = {'Films (English)', 'Films (Non-English)', 'TV (English)', 'TV (Non-English)'}

# name: (expected header, week column or None, minimum data rows)
FILES = {
    'all-weeks-global': (
        ['week', 'category', 'weekly_rank', 'show_title', 'season_title', 'weekly_hours_viewed',
         'runtime', 'weekly_views', 'cumulative_weeks_in_top_10'],
        'week', 11000),
    'all-weeks-countries': (
        ['country_name', 'country_iso2', 'week', 'category', 'weekly_rank', 'show_title',
         'season_title', 'cumulative_weeks_in_top_10'],
        'week', 500000),
    'most-popular': (
        ['category', 'rank', 'show_title', 'season_title', 'hours_viewed_first_91_days',
         'runtime', 'views_first_91_days'],
        None, 40),
}


def open_download(url):
    # GET with stream=True so the headers can be read before downloading the body
    # (the server answers HEAD with 403). Retries 5xx, 429 and connection errors.
    for attempt in range(4):
        try:
            response = requests.get(url, headers=HEADERS, timeout=120, stream=True)
            if response.status_code == 429 or response.status_code >= 500:
                raise requests.HTTPError(f'{response.status_code} from {url}', response=response)
            response.raise_for_status()  # any other 4xx fails right away
            return response
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            status = getattr(getattr(e, 'response', None), 'status_code', None)
            if status is not None and 400 <= status < 500 and status != 429:
                raise
            if attempt == 3:
                raise
            print(f'Attempt {attempt + 1} for {url} failed ({e}), retrying')
            time.sleep(15 * (attempt + 1))


def validate(name, text):
    # fail loudly on an error page, a truncated download, or a changed layout
    header, week_col, min_rows = FILES[name]
    lines = text.split('\n')
    if lines and lines[-1] == '':
        lines = lines[:-1]
    got_header = lines[0].rstrip('\r').split('\t') if lines else []
    if got_header != header:
        raise RuntimeError(f'{name}: unexpected header {got_header}')
    rows = [line.rstrip('\r').split('\t') for line in lines[1:]]
    if len(rows) < min_rows:
        raise RuntimeError(f'{name}: only {len(rows):,} rows, expected at least {min_rows:,}')
    bad = [r for r in rows if len(r) != len(header)]
    if bad:
        raise RuntimeError(f'{name}: {len(bad)} rows have the wrong number of columns, e.g. {bad[0]}')
    if week_col is None:
        return len(rows), None
    i = header.index(week_col)
    weeks = {r[i] for r in rows}
    latest = max(weeks)
    latest_date = datetime.strptime(latest, '%Y-%m-%d').date()
    if latest_date.weekday() != 6:
        raise RuntimeError(f'{name}: latest week {latest} is not a Sunday')
    if datetime.now(timezone.utc).date() - latest_date > timedelta(days=45):
        raise RuntimeError(f'{name}: latest week {latest} is more than 45 days old')
    if name == 'all-weeks-global':
        latest_rows = [r for r in rows if r[i] == latest]
        if len(latest_rows) != 40 or {r[1] for r in latest_rows} != CATEGORIES_GLOBAL:
            raise RuntimeError(f'{name}: week {latest} has {len(latest_rows)} rows, expected 10 per category')
    return len(rows), latest


dry_run = bool(os.environ.get('DRY_RUN'))  # DRY_RUN=1 python jobs/netflix-top-10.py checks without S3
if not dry_run:
    s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

for name in FILES:
    url = BASE_URL + name + '.tsv'
    response = open_download(url)
    last_modified = response.headers.get('Last-Modified')
    if last_modified:
        source_time = parsedate_to_datetime(last_modified).astimezone(timezone.utc)
    else:
        source_time = datetime.now(timezone.utc)
        print(f'{name}: no Last-Modified header; naming the file after the download time')
    # named after when Netflix last updated the file (UTC)
    file_path = (PREFIX + name + '/netflix-top-10-' + name + '-'
                 + source_time.strftime('%Y-%m-%d-%H-%M-%S') + '.tsv.gz')

    if not dry_run:
        try:
            s3.head_object(Bucket=BUCKET, Key=file_path)
            response.close()
            print(f'{file_path} already saved; Netflix has not published a new release since. Skipping.')
            continue
        except ClientError as e:
            if e.response['Error']['Code'] not in ('404', 'NoSuchKey', 'NotFound'):
                raise

    content = response.content
    expected_length = response.headers.get('Content-Length')
    # requests decompresses gzip transfer encoding, so the length only compares when there was none
    compressed = response.headers.get('Content-Encoding') not in (None, 'identity')
    if expected_length and not compressed and int(expected_length) != len(content):
        raise RuntimeError(f'{name}: got {len(content):,} bytes, expected {int(expected_length):,}')
    if 'tab-separated' not in response.headers.get('Content-Type', ''):
        raise RuntimeError(f'{name}: unexpected Content-Type {response.headers.get("Content-Type")}')
    n_rows, latest_week = validate(name, content.decode('utf-8'))
    body = gzip.compress(content, compresslevel=9)
    summary = f'{n_rows:,} rows' + (f', latest week {latest_week}' if latest_week else '')

    if dry_run:
        print(f'Dry run: would save {name} ({summary}, {len(content):,} bytes, '
              f'{len(body):,} gzipped) to {file_path}')
        continue

    s3.upload_fileobj(
        BytesIO(body), BUCKET, file_path,
        ExtraArgs={
            'ContentType': 'application/gzip',
            'Metadata': {
                'source-url': url,
                'source-last-modified': last_modified or '',
                'latest-week': latest_week or '',
                'rows': str(n_rows),
                'downloaded-at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            },
        },
    )
    print(f'Copy {name} ({summary}) to S3 Bucket {BUCKET} at {file_path}, Done!')
