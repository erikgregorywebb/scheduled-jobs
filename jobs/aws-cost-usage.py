import boto3
from botocore.config import Config
import pandas as pd
from datetime import datetime, date, timedelta, timezone
from io import StringIO
import os

# get environmental variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# get current datetime label
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# settings
BUCKET = 'egw-data-dumps'
PREFIX = 'data-pipeline-logs/'
# AWS revises recent days for up to ~24 hours, and month-to-date credits and
# refunds can land later, so each daily run re-pulls a trailing window
LOOKBACK_DAYS = 14
# optional backfill: set AWS_USAGE_START / AWS_USAGE_END (YYYY-MM-DD, end exclusive)
# to pull any range within Cost Explorer's ~14 months of history
end = date.fromisoformat(os.environ.get('AWS_USAGE_END') or date.today().isoformat())
start = date.fromisoformat(os.environ.get('AWS_USAGE_START') or (end - timedelta(days=LOOKBACK_DAYS)).isoformat())
if start >= end:
    raise SystemExit(f'Start {start} must be before end {end}')

# Cost Explorer is a global service served from us-east-1; boto3's standard
# retry mode backs off on throttling, 5xx and connection errors
ce = boto3.client(
    'ce', region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    config=Config(retries={'max_attempts': 5, 'mode': 'standard'}, connect_timeout=30, read_timeout=60),
)

# pull daily cost and usage per service and usage type
# (each request, including each page, is billed at $0.01)
METRICS = ['UnblendedCost', 'NetUnblendedCost', 'UsageQuantity']
rows = []
periods = 0
token = None
while True:
    kwargs = dict(
        TimePeriod={'Start': start.isoformat(), 'End': end.isoformat()},
        Granularity='DAILY',
        Metrics=METRICS,
        GroupBy=[
            {'Type': 'DIMENSION', 'Key': 'SERVICE'},
            {'Type': 'DIMENSION', 'Key': 'USAGE_TYPE'},
        ],
    )
    if token:
        kwargs['NextPageToken'] = token
    resp = ce.get_cost_and_usage(**kwargs)
    for period in resp['ResultsByTime']:
        periods += 1
        for g in period['Groups']:
            m = g['Metrics']
            rows.append({
                'date': period['TimePeriod']['Start'],
                'estimated': period['Estimated'],
                'service': g['Keys'][0],
                'usage_type': g['Keys'][1],
                'unblended_cost': float(m['UnblendedCost']['Amount']),
                'net_unblended_cost': float(m['NetUnblendedCost']['Amount']),
                'cost_unit': m['UnblendedCost']['Unit'],
                'usage_quantity': float(m['UsageQuantity']['Amount']),
                'usage_unit': m['UsageQuantity']['Unit'],
            })
    token = resp.get('NextPageToken')
    if not token:
        break

# one result period per day is expected; fewer means a truncated response
expected = (end - start).days
if periods < expected:
    raise SystemExit(f'Expected {expected} daily periods, got {periods}')

columns = ['date', 'estimated', 'service', 'usage_type', 'unblended_cost', 'net_unblended_cost',
           'cost_unit', 'usage_quantity', 'usage_unit']
df = pd.DataFrame(rows, columns=columns)
df['collected_at_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
print(f'{len(df)} rows across {periods} days ({start} to {end}, end exclusive); '
      f'total unblended cost {df["unblended_cost"].sum():.2f} USD')

# copy to s3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

file_path = PREFIX + 'aws-cost-usage-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df, bucket=BUCKET, filepath=file_path)
