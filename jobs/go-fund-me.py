import pandas as pd
from bs4 import BeautifulSoup
import json
import re
import time

import common
from common import RUN_DATETIME, RUN_LABEL, save_csv

# GoFundMe's discover page no longer lists trending fundraisers in its HTML (they
# load later via JavaScript), but it does link a few featured fundraisers, and each
# fundraiser page embeds its data in the page's __NEXT_DATA__ JSON.
NUM_FEATURED = 3
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}

def get(url):
    return common.get(url, headers=HEADERS, timeout=30)

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
        'datetime': RUN_DATETIME,
    })

df = pd.DataFrame(rows)

file_path = 'go-fund-me/go-fund-me-' + RUN_LABEL + '.csv'
save_csv(df, file_path)
