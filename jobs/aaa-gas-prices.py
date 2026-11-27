import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO
import json
import re
import time
import os

# Collects AAA's daily gas price averages from gasprices.aaa.com and dumps three files to S3
# under aaa-gas-prices/:
#   - averages: national, state (50 + DC), and metro-area averages by grade, for each of the
#     periods AAA shows (current, yesterday, week ago, month ago, year ago)
#   - records: the record-high regular and diesel prices (with dates) for the nation, each state
#     and each metro
#   - counties: the county-level regular price behind each state page's map
# AAA only shows today's numbers (plus the four lookback points), and keeps no public history
# by state, metro or county, so these files are the history.
# robots.txt asks for a 10-second crawl delay (checked 2026-11-27), which this honours:
# 103 requests take about 17 minutes. The site is behind Cloudflare, which returns 403 to
# non-browser User-Agents, so a browser-style one is sent.

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}
BASE_URL = 'https://gasprices.aaa.com/'
CRAWL_DELAY = float(os.environ.get('CRAWL_DELAY', 10))
BUCKET = 'egw-data-dumps'
PREFIX = 'aaa-gas-prices/'

STATES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'DC': 'District of Columbia',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois',
    'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana',
    'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota',
    'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon',
    'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota',
    'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia',
    'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
}
PERIODS = {
    'current avg.': 'current', 'yesterday avg.': 'yesterday', 'week ago avg.': 'week_ago',
    'month ago avg.': 'month_ago', 'year ago avg.': 'year_ago',
}
GRADES = {'regular': 'regular', 'mid-grade': 'mid_grade', 'mid': 'mid_grade',
          'premium': 'premium', 'diesel': 'diesel', 'e85': 'e85'}
GRADE_COLUMNS = ['regular', 'mid_grade', 'premium', 'diesel', 'e85']
RECORD_GRADES = {'regular unleaded': 'regular', 'diesel': 'diesel'}

now = datetime.now()
current_datetime = now.strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = now.strftime('%Y-%m-%d-%H-%M-%S')

dry_run = bool(os.environ.get('DRY_RUN'))  # local check without S3: DRY_RUN=1 python jobs/aaa-gas-prices.py
if not dry_run:
    s3 = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

session = requests.Session()
session.headers.update(HEADERS)
last_request = 0.0

def fetch(url, params=None, attempts=4):
    # one polite, retried GET; retries 429/5xx and connection errors, fails fast on other 4xx
    global last_request
    for attempt in range(attempts):
        wait = CRAWL_DELAY - (time.time() - last_request)
        if wait > 0:
            time.sleep(wait)
        last_request = time.time()
        try:
            r = session.get(url, params=params, timeout=60)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f'{r.status_code} from {r.url}')
            r.raise_for_status()
            return r.text
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            retryable = not (isinstance(e, requests.HTTPError) and e.response is not None and e.response.status_code < 500 and e.response.status_code != 429)
            if attempt == attempts - 1 or not retryable:
                raise
            print(f'  retrying {url} {params or ""} after error: {e}')
            time.sleep(30 * (attempt + 1))

def to_price(text):
    text = text.strip().replace('$', '').replace(',', '')
    if text in ('', 'N/A', '--'):
        return None
    price = float(text)
    if not 0.5 < price < 20:
        raise ValueError(f'implausible price: {text}')
    return price

def to_date(text):
    # AAA writes dates like 11/26/26 or 6/14/22
    return datetime.strptime(text.strip(), '%m/%d/%y').strftime('%Y-%m-%d')

def price_as_of(soup):
    match = re.search(r'Price as of\s+(\d{1,2}/\d{1,2}/\d{2})', soup.get_text(' '))
    if not match:
        raise ValueError('no "Price as of" date on page')
    return to_date(match.group(1))

def parse_averages(table):
    # the 5-row grid of periods x grades; returns {period: {grade: price}}
    header = [GRADES[th.get_text(strip=True).lower()] for th in table.find('thead').find_all('th')[1:]]
    out = {}
    for tr in table.find('tbody').find_all('tr'):
        cells = tr.find_all('td')
        period = PERIODS[cells[0].get_text(strip=True).lower()]
        out[period] = {grade: to_price(td.get_text()) for grade, td in zip(header, cells[1:])}
    if 'current' not in out:
        raise ValueError('averages table has no current row')
    return out

def parse_records(table):
    rows = []
    for tr in table.find_all('tr'):
        cells = [td.get_text(strip=True) for td in tr.find_all('td')]
        if not cells or cells[0].lower() not in RECORD_GRADES:
            continue
        rows.append((RECORD_GRADES[cells[0].lower()], to_price(cells[-2]), to_date(cells[-1])))
    return rows

def following_records(table):
    nxt = table.find_next('table')
    return parse_records(nxt) if nxt is not None and 'record-table' in (nxt.get('class') or []) else []

averages, records, counties = [], [], []

def add(level, state, state_name, metro, as_of, table):
    for period, prices in parse_averages(table).items():
        averages.append({'level': level, 'state': state, 'state_name': state_name, 'metro': metro,
                         'period': period, **{g: prices.get(g) for g in GRADE_COLUMNS},
                         'price_as_of': as_of, 'datetime': current_datetime})
    for grade, price, date in following_records(table):
        records.append({'level': level, 'state': state, 'state_name': state_name, 'metro': metro,
                        'grade': grade, 'record_price': price, 'record_date': date,
                        'price_as_of': as_of, 'datetime': current_datetime})

# national
soup = BeautifulSoup(fetch(BASE_URL), 'html.parser')
national_as_of = price_as_of(soup)
add('national', 'US', 'United States', None, national_as_of, soup.find('table', class_='table-mob'))
print(f'national: price as of {national_as_of}')

# states, their metros, and their counties
failed = []
for code, name in STATES.items():
    try:
        html = fetch(BASE_URL, params={'state': code})
        soup = BeautifulSoup(html, 'html.parser')
        heading = soup.find('h1', class_='nati')
        if heading is None or name.lower() not in heading.get_text(' ', strip=True).lower():
            raise ValueError(f'page heading is not {name}: {heading.get_text(" ", strip=True) if heading else None}')
        as_of = price_as_of(soup)
        add('state', code, name, None, as_of, heading.find_next('table', class_='table-mob'))

        metro_count = 0
        accordion = soup.find(class_='metros-js')
        for h3 in accordion.find_all('h3') if accordion else []:
            add('metro', code, name, h3.get_text(strip=True), as_of, h3.find_next('table', class_='table-mob'))
            metro_count += 1

        # county prices live in the map plugin's config script, one map per state
        county_count = 0
        map_id = re.search(r'premiumhtml5map_js_data=true(?:&#038;|&amp;|&)map_id=(\d+)', html)
        if map_id:
            cfg = fetch(BASE_URL + 'index.php', params={'premiumhtml5map_js_data': 'true', 'map_id': map_id.group(1)})
            for county, comment in re.findall(r'"st\d+":\{"id":\d+,"name":"((?:[^"\\]|\\.)*)",[^{}]*?"comment":"((?:[^"\\]|\\.)*)"', cfg):
                counties.append({'state': code, 'state_name': name,
                                 'county': json.loads(f'"{county}"'),
                                 'regular': to_price(comment), 'price_as_of': as_of,
                                 'datetime': current_datetime})
                county_count += 1
        print(f'{code}: price as of {as_of}, {metro_count} metros, {county_count} counties')
    except Exception as e:
        print(f'FAILED {code}: {e!r}')
        failed.append(code)

def save(name, rows):
    df = pd.DataFrame(rows)
    filename = f'aaa-gas-prices-{name}-{current_datetime_label}.csv'
    if dry_run:
        df.to_csv(filename, index=False)
        print(f'Dry run: wrote {df.shape[0]} rows to {filename}')
    else:
        csv_buf = StringIO()
        df.to_csv(csv_buf, header=True, index=False)
        s3.put_object(Bucket=BUCKET, Body=csv_buf.getvalue(), Key=f'{PREFIX}{name}/{filename}')
        print(f'Copy {df.shape[0]} rows to S3 Bucket {BUCKET} at {PREFIX}{name}/{filename}, Done!')

# don't save a run where the site mostly failed (e.g. Cloudflare blocking the runner)
if len(failed) > 5:
    raise SystemExit(f'Too many states failed, nothing saved: {", ".join(failed)}')

save('averages', averages)
save('records', records)
save('counties', counties)

if failed:
    raise SystemExit(f'Some states failed (the rest were saved): {", ".join(failed)}')
