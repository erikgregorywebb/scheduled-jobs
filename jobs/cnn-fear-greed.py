import requests
import pandas as pd
from datetime import datetime, timezone
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

# settings
# the JSON behind https://www.cnn.com/markets/fear-and-greed
URL = 'https://production.dataviz.cnn.io/index/fearandgreed/graphdata'
# without browser-like headers the server answers 418 "I'm a teapot. You're a bot."
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.cnn.com/',
    'Origin': 'https://www.cnn.com',
}
BUCKET = 'egw-data-dumps'
PREFIX = 'cnn-fear-greed/'
# the headline index plus its seven components (two of which carry a second series)
INDICATORS = [
    'fear_and_greed_historical',
    'market_momentum_sp500', 'market_momentum_sp125',
    'stock_price_strength', 'stock_price_breadth',
    'put_call_options',
    'market_volatility_vix', 'market_volatility_vix_50',
    'junk_bond_demand', 'safe_haven_demand',
]
# each series covers about a year of trading days; far fewer means a truncated response
MIN_POINTS = 200

# download, retrying transient failures and fast-failing other 4xx
content = None
for attempt in range(1, 4):
    try:
        r = requests.get(URL, headers=HEADERS, timeout=60)
        if r.status_code == 200:
            content = r.json()
            break
        if r.status_code < 500 and r.status_code != 429:
            raise SystemExit(f'Request failed with status {r.status_code}: {r.text[:200]}')
        print(f'Attempt {attempt}: status {r.status_code}')
    except (requests.RequestException, ValueError) as e:
        print(f'Attempt {attempt}: {e}')
    time.sleep(20 * attempt)
if content is None:
    raise SystemExit('Fear & Greed request failed after 3 attempts')

missing = [k for k in ['fear_and_greed'] + INDICATORS if k not in content]
if missing:
    raise SystemExit(f'Response is missing {missing}; got {sorted(content)}')

def ms_to_utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

# summary: one row per indicator with its current score and rating
headline = content['fear_and_greed']
summary_rows = []
for name in INDICATORS:
    ind = content[name]
    summary_rows.append([
        name,
        ind.get('score'),
        ind.get('rating'),
        ms_to_utc(ind['timestamp']) if ind.get('timestamp') else None,
        len(ind.get('data', [])),
        current_datetime,
    ])
summary = pd.DataFrame(summary_rows, columns=[
    'indicator', 'score', 'rating', 'timestamp_utc', 'n_points', 'datetime',
])
# the headline block also has comparison scores the components don't
for col in ['previous_close', 'previous_1_week', 'previous_1_month', 'previous_1_year']:
    summary[col] = None
    summary.loc[summary['indicator'] == 'fear_and_greed_historical', col] = headline.get(col)

# history: one row per indicator per data point (~1 year of trading days, rolling)
history_rows = []
for name in INDICATORS:
    points = content[name].get('data', [])
    if len(points) < MIN_POINTS:
        raise SystemExit(f'{name} has only {len(points)} data points; expected at least {MIN_POINTS}')
    for p in points:
        history_rows.append([name, ms_to_utc(p['x']), p['y'], p.get('rating'), current_datetime])
history = pd.DataFrame(history_rows, columns=['indicator', 'timestamp_utc', 'value', 'rating', 'datetime'])

print(f"Fear & Greed: {headline['score']:.1f} ({headline['rating']}) as of {headline['timestamp']}")

# copy to s3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

copy_to_s3(client=s3, df=summary, bucket=BUCKET, filepath=PREFIX + 'cnn-fear-greed-summary-' + current_datetime_label + '.csv')
copy_to_s3(client=s3, df=history, bucket=BUCKET, filepath=PREFIX + 'cnn-fear-greed-history-' + current_datetime_label + '.csv')
