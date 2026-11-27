import requests
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO
import json
import os
import time

# Snapshots prices for the most active open markets on Polymarket from its public Gamma API
# (https://docs.polymarket.com/), so each market's odds can be followed day to day. No key is needed.
#
# Polymarket has 70,000+ open markets, most with no trading, and pulling them all is ~0.5 GB a run.
# Instead this takes the top TOP_N open markets by each of 24-hour volume, total volume and liquidity
# and keeps their union (~4,000 markets in Nov 2026; ~60 requests, ~45 MB, ~20 seconds).
# Ranking by total volume and liquidity, not just the last day's trading, keeps long-running markets
# (elections, economics) in every snapshot so their prices form a continuous series.
#
# Output (one file per run):
#   polymarket/markets-<datetime>.csv  one row per market, with each outcome's price

# get environmental variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# get current date and datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

url = 'https://gamma-api.polymarket.com/markets'
TOP_N = 2000
PAGE_SIZE = 100  # the API's maximum
RANKINGS = {'volume24hr': 'rank_volume_24hr', 'volumeNum': 'rank_volume', 'liquidityNum': 'rank_liquidity'}

def get_page(order, offset, attempts=3):
    params = {'active': 'true', 'closed': 'false', 'limit': PAGE_SIZE, 'offset': offset, 'order': order, 'ascending': 'false'}
    for attempt in range(1, attempts + 1):
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code == 200:
                page = r.json()
                if not isinstance(page, list):
                    raise SystemExit(f'{order} offset {offset}: expected a list of markets, got {str(page)[:200]}')
                return page
            if 400 <= r.status_code < 500 and r.status_code != 429:
                # a bad request won't fix itself on retry
                raise SystemExit(f'{order} offset {offset}: status {r.status_code}: {r.text[:200]}')
            print(f'{order} offset {offset} attempt {attempt}: status {r.status_code}')
        except (requests.RequestException, ValueError) as e:
            print(f'{order} offset {offset} attempt {attempt}: {e}')
        time.sleep(10 * attempt)
    raise SystemExit(f'Polymarket request for {order} offset {offset} failed after {attempts} attempts')

def parse_list(s):
    # outcomes, prices and token ids come back as JSON-encoded strings
    try:
        return json.loads(s) if isinstance(s, str) else (s or [])
    except ValueError:
        return []

def to_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

markets = {}  # market id -> market
ranks = {}    # market id -> {rank column: rank}
for order, rank_col in RANKINGS.items():
    for offset in range(0, TOP_N, PAGE_SIZE):
        page = get_page(order, offset)
        for i, m in enumerate(page):
            markets[m['id']] = m
            ranks.setdefault(m['id'], {})[rank_col] = offset + i + 1
        if len(page) < PAGE_SIZE:
            break
        time.sleep(0.2)
    print(f'Fetched top {TOP_N} markets by {order}; {len(markets)} unique so far')
if not markets:
    raise SystemExit('No markets returned')

rows = []
for market_id, m in markets.items():
    event = (m.get('events') or [{}])[0]
    outcomes = parse_list(m.get('outcomes'))
    prices = parse_list(m.get('outcomePrices'))
    row = {
        'market_id': market_id,
        'condition_id': m.get('conditionId'),
        'slug': m.get('slug'),
        'question': m.get('question'),
        'group_item_title': m.get('groupItemTitle'),
        'event_id': event.get('id'),
        'event_slug': event.get('slug'),
        'event_title': event.get('title'),
        'series_slug': event.get('seriesSlug'),
        'sports_market_type': m.get('sportsMarketType'),
        'outcome_count': len(outcomes),
        # nearly every market is binary (Yes/No, Over/Under, team vs team); the full lists are kept below
        'outcome_1': outcomes[0] if len(outcomes) > 0 else None,
        'outcome_1_price': to_float(prices[0]) if len(prices) > 0 else None,
        'outcome_2': outcomes[1] if len(outcomes) > 1 else None,
        'outcome_2_price': to_float(prices[1]) if len(prices) > 1 else None,
        'outcomes': json.dumps(outcomes),
        'outcome_prices': json.dumps(prices),
        'last_trade_price': to_float(m.get('lastTradePrice')),
        'best_bid': to_float(m.get('bestBid')),
        'best_ask': to_float(m.get('bestAsk')),
        'spread': to_float(m.get('spread')),
        'one_hour_price_change': to_float(m.get('oneHourPriceChange')),
        'one_day_price_change': to_float(m.get('oneDayPriceChange')),
        'one_week_price_change': to_float(m.get('oneWeekPriceChange')),
        'volume_24hr': to_float(m.get('volume24hr')),
        'volume_1wk': to_float(m.get('volume1wk')),
        'volume_1mo': to_float(m.get('volume1mo')),
        'volume': to_float(m.get('volumeNum')),
        'liquidity': to_float(m.get('liquidityNum')),
        'event_volume': to_float(event.get('volume')),
        'event_open_interest': to_float(event.get('openInterest')),
        'neg_risk': m.get('negRisk'),
        'accepting_orders': m.get('acceptingOrders'),
        'start_date': m.get('startDate'),
        'end_date': m.get('endDate'),
        'game_start_time': m.get('gameStartTime'),
        'created_at': m.get('createdAt'),
        'updated_at': m.get('updatedAt'),
        'clob_token_ids': json.dumps(parse_list(m.get('clobTokenIds'))),
        'scraped_at': current_datetime,
    }
    for rank_col in RANKINGS.values():
        row[rank_col] = ranks[market_id].get(rank_col)
    rows.append(row)

df = pd.DataFrame(rows).astype({c: 'Int64' for c in RANKINGS.values()})
df = df.sort_values('volume_24hr', ascending=False)
print(f'Parsed {len(df)} markets across {df["event_id"].nunique()} events')

# copy to s3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath='polymarket/markets-' + current_datetime_label + '.csv')
