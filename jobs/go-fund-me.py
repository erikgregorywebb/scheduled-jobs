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

# GoFundMe's discover page no longer lists trending fundraisers in its HTML (they
# load later via JavaScript), but it does link a few featured fundraisers, and each
# fundraiser page embeds its data in the page's __NEXT_DATA__ JSON.
NUM_FEATURED = 3
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}

# get current datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

def get(url):
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response

def next_data(html):
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not match:
        raise ValueError('no __NEXT_DATA__ on page')
    return json.loads(match.group(1))

def money(value):
    return (value or {}).get('amount'), (value or {}).get('currencyCode')

def html_to_text(html):
    return BeautifulSoup(html or '', 'html.parser').get_text('\n', strip=True)

# featured fundraisers, in the order the discover page links them
discover = get('https://www.gofundme.com/discover').text
slugs = list(dict.fromkeys(re.findall(r'/f/([a-z0-9][a-z0-9-]+)', discover)))[:NUM_FEATURED]
if not slugs:
    raise RuntimeError('No featured fundraisers found on the GoFundMe discover page')

# fundraiser details
rows = []
for rank, slug in enumerate(slugs, start=1):
    time.sleep(1)
    response = get(f'https://www.gofundme.com/f/{slug}')
    final_slug = response.url.rstrip('/').split('/f/')[-1].split('?')[0]
    state = next_data(response.text)['props']['pageProps']['__APOLLO_STATE__']

    fundraisers = [v for k, v in state.items() if k.startswith('Fundraiser:')]
    matches = [f for f in fundraisers if {f.get('slug'), f.get('defaultSlug')} & {slug, final_slug}]
    if not matches and len(fundraisers) != 1:
        raise RuntimeError(f'Could not identify fundraiser data for {slug}')
    f = (matches or fundraisers)[0]

    organizer = state.get((f.get('organizer') or {}).get('__ref', ''), {})
    location = f.get('location') or {}
    raised, currency = money(f.get('currentAmount'))
    goal, _ = money(f.get('goalAmount'))
    description = next((v for k, v in f.items() if k.startswith('description') and '"excerpt":false' in k), None)

    rows.append({
        'rank': rank,
        'title': f.get('title'),
        'slug': f.get('slug') or final_slug,
        'url': f'https://www.gofundme.com/f/{f.get("slug") or final_slug}',
        'raised_amount': raised,
        'goal_amount': goal,
        'currency': currency,
        'donation_count': f.get('donationCount'),
        'donations_last_24h': f.get('donationsInLast24HoursCount'),
        'heart_count': f.get('heartCount'),
        'status': f.get('status'),
        'donations_enabled': f.get('donationsEnabled'),
        'organizer': organizer.get('name') or ' '.join(filter(None, [organizer.get('firstName'), organizer.get('lastName')])) or None,
        'city': location.get('city'),
        'state': location.get('statePrefix'),
        'country': location.get('countryCode'),
        'category_id': f.get('categoryId'),
        'created_at': f.get('createdAt'),
        'published_at': f.get('publishedAt'),
        'summary': f.get('storySlideSummary'),
        'description': html_to_text(description),
        'datetime': current_datetime,
    })

df = pd.DataFrame(rows)

# get environmental variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# copy to s3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

file_path = 'go-fund-me/go-fund-me-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath=file_path)
