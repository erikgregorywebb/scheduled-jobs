import pandas as pd
from datetime import date
import xml.etree.ElementTree as ET
import time
import os

from common import BUCKET, DRY_RUN, RUN_DATETIME, RUN_LABEL, RUN_TIME, s3, save_csv
import common

# Collects daily US Treasury data and dumps each dataset to S3 under treasury/:
#   - yield curves from home.treasury.gov (par nominal, bill rates, long-term, real/TIPS)
#   - auction results and the upcoming-auction list from TreasuryDirect
#   - Debt to the Penny and the daily cash balance from the Fiscal Data API
# Treasury keeps full history for everything except the upcoming-auction list,
# so daily runs pull a recent window and overlap; dedupe downstream on the date column.
# The first run (no yield-curve files in S3 yet) backfills the yield curves from 1990.
# robots.txt on home.treasury.gov allows all paths (checked 2026-11-27).

HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; scheduled-jobs data collector)'}
PREFIX = 'treasury/'
BACKFILL_START_YEAR = 1990

YIELD_URL = 'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml'
YIELD_DATASETS = {
    'par-yield-curve': 'daily_treasury_yield_curve',
    'bill-rates': 'daily_treasury_bill_rates',
    'long-term-rates': 'daily_treasury_long_term_rate',
    'real-yield-curve': 'daily_treasury_real_yield_curve',
}
AUCTIONS_URL = 'https://www.treasurydirect.gov/TA_WS/securities/'
FISCAL_URL = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/'

ATOM = '{http://www.w3.org/2005/Atom}'
META = '{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}'

now = RUN_TIME  # UTC

def get(url, params):
    # 3 attempts on connection errors, 5xx and 429; other 4xx fail fast
    return common.get(url, params=params, headers=HEADERS, timeout=90, delays=(10, 20))

def parse_yield_xml(content):
    rows = []
    for entry in ET.fromstring(content).iter(ATOM + 'entry'):
        props = entry.find(f'{ATOM}content/{META}properties')
        rows.append({child.tag.split('}')[1].lower(): child.text for child in props})
    return rows

def get_yield_rows(dataset, period_param, period_value):
    response = get(YIELD_URL, {'data': dataset, period_param: period_value})
    time.sleep(1)
    return parse_yield_xml(response.content)

def recent_months():
    first_of_month = date(now.year, now.month, 1)
    previous = date(now.year - 1, 12, 1) if now.month == 1 else date(now.year, now.month - 1, 1)
    return [previous.strftime('%Y%m'), first_of_month.strftime('%Y%m')]

def s3_has_files(prefix):
    return s3().list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1).get('KeyCount', 0) > 0

if DRY_RUN:
    # local check without touching S3: DRY_RUN=1 python jobs/treasury-daily.py
    # (files go to ./out/; set BACKFILL=1 to try the full backfill)
    backfill = bool(os.environ.get('BACKFILL'))
else:
    backfill = not s3_has_files(PREFIX + 'treasury-par-yield-curve-')

def save(name, rows):
    df = pd.DataFrame(rows)
    df['datetime'] = RUN_DATETIME
    save_csv(df, PREFIX + f'treasury-{name}-{RUN_LABEL}.csv')

# yield curves
def collect_yields():
    for name, dataset in YIELD_DATASETS.items():
        rows = []
        if backfill:
            for year in range(BACKFILL_START_YEAR, now.year + 1):
                rows += get_yield_rows(dataset, 'field_tdr_date_value', year)
            print(f'{name}: backfilled {len(rows)} rows from {BACKFILL_START_YEAR}')
        else:
            for month in recent_months():
                rows += get_yield_rows(dataset, 'field_tdr_date_value_month', month)
            print(f'{name}: {len(rows)} rows for {", ".join(recent_months())}')
        # drop Treasury's internal row ids
        rows = [{k: v for k, v in row.items() if k != 'id' and not k.endswith('dataid')} for row in rows]
        save(name, rows)

# auctions: results from the last 7 days, plus the announced-but-not-yet-held list
def collect_auctions():
    auctioned = get(AUCTIONS_URL + 'auctioned', {'format': 'json', 'days': 7}).json()
    print(f'auctions-recent: {len(auctioned)} auctions')
    save('auctions-recent', auctioned)
    upcoming = get(AUCTIONS_URL + 'upcoming', {'format': 'json'}).json()
    print(f'auctions-upcoming: {len(upcoming)} auctions')
    save('auctions-upcoming', upcoming)

# fiscal data: total debt and the daily cash balance (several account rows per day)
def collect_fiscal():
    for name, endpoint, page_size in [
        ('debt-to-penny', 'v2/accounting/od/debt_to_penny', 30),
        ('cash-balance', 'v1/accounting/dts/operating_cash_balance', 200),
    ]:
        params = {'sort': '-record_date', 'page[size]': page_size}
        data = get(FISCAL_URL + endpoint, params).json()['data']
        print(f'{name}: {len(data)} rows')
        save(name, data)

# run each source separately so one outage doesn't lose the others.
# Fiscal Data is optional: its API keeps full history, so a missed day is
# recovered on the next run that succeeds, and the API has had long outages
# (all 504s on 2026-11-27), so a failure there only logs a warning.
errors = []
for collector, required in [(collect_yields, True), (collect_auctions, True), (collect_fiscal, False)]:
    try:
        collector()
    except Exception as e:
        if required:
            print(f'FAILED {collector.__name__}: {e!r}')
            errors.append(collector.__name__)
        else:
            print(f'WARNING {collector.__name__} skipped, will retry next run: {e!r}')

if errors:
    raise SystemExit(f'Some Treasury sources failed: {", ".join(errors)}')
