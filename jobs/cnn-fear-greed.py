import requests
import pandas as pd
from datetime import datetime, timezone

from common import RUN_DATETIME, RUN_LABEL, get, save_csv

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
try:
    content = get(URL, headers=HEADERS, timeout=60, delays=(20, 40), name='Fear & Greed').json()
except requests.HTTPError as e:
    raise SystemExit(f'Request failed with status {e.response.status_code}: {e.response.text[:200]}')

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
        RUN_DATETIME,
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
        history_rows.append([name, ms_to_utc(p['x']), p['y'], p.get('rating'), RUN_DATETIME])
history = pd.DataFrame(history_rows, columns=['indicator', 'timestamp_utc', 'value', 'rating', 'datetime'])

print(f"Fear & Greed: {headline['score']:.1f} ({headline['rating']}) as of {headline['timestamp']}")

save_csv(summary, PREFIX + 'cnn-fear-greed-summary-' + RUN_LABEL + '.csv')
save_csv(history, PREFIX + 'cnn-fear-greed-history-' + RUN_LABEL + '.csv')
