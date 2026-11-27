import requests
import pandas as pd
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from urllib.parse import urljoin
import re

from common import RUN_DATETIME, RUN_LABEL, get, save_csv

# Collects the 25 latest press releases and announcements from the official RSS feed of the
# Church of Jesus Christ of Latter-day Saints' Newsroom, https://newsroom.churchofjesuschrist.org/rss.
# No token is needed. The feed covers roughly the last two to three weeks, so a daily run
# catches every article; dedupe on `link` when reading the files back.
#
# Each item's <description> packs three fields into one string:
#   'SALT LAKE CITY | Wednesday, 25 November 2026 | <p>Summary...</p><img src='/media/960x540/x.jpg'/>'
# which are split here into dateline, summary (HTML stripped) and image_url.
#
# Output: lds-newsroom/lds-newsroom-rss-<datetime>.csv, one row per article.

# settings
URL = 'https://newsroom.churchofjesuschrist.org/rss'
SITE = 'https://newsroom.churchofjesuschrist.org/'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'Accept': 'application/rss+xml, application/xml, text/xml, */*',
}
PREFIX = 'lds-newsroom/'
# the feed always lists 25 items; far fewer means a truncated or broken response
MIN_ITEMS = 10

# download, retrying transient failures and fast-failing other 4xx
try:
    xml = get(URL, headers=HEADERS, timeout=60, delays=(20, 40), name='Newsroom RSS').content
except requests.HTTPError as e:
    raise SystemExit(f'Request failed with status {e.response.status_code}: {e.response.text[:200]}')

root = ET.fromstring(xml)
items = root.findall('./channel/item')
if len(items) < MIN_ITEMS:
    raise SystemExit(f'Feed has only {len(items)} items; expected at least {MIN_ITEMS}')

def https(url):
    # the feed lists links as http://, which just redirect to https://
    return re.sub(r'^http://', 'https://', url.strip()) if url else None

rows = []
for item in items:
    link = https(item.findtext('link'))
    description = item.findtext('description') or ''
    # split 'DATELINE | Weekday, DD Month YYYY | <html>'; fall back to the whole string as the body
    parts = description.split(' | ', 2)
    if len(parts) == 3:
        dateline, display_date, body = parts
    else:
        dateline, display_date, body = None, None, description
    soup = BeautifulSoup(body, 'html.parser')
    img = soup.find('img')
    image_url = urljoin(SITE, img['src']) if img and img.get('src') else None
    summary = re.sub(r'\s+', ' ', soup.get_text(' ')).strip() or None
    rows.append([
        item.findtext('pubDate'),
        (item.findtext('title') or '').strip() or None,
        link,
        link.rstrip('/').rsplit('/', 1)[-1] if link else None,
        dateline.strip() if dateline else None,
        display_date.strip() if display_date else None,
        summary,
        image_url,
        RUN_DATETIME,
    ])

df = pd.DataFrame(rows, columns=[
    'pub_date', 'title', 'link', 'slug', 'dateline', 'display_date', 'summary', 'image_url', 'datetime',
])
print(f"{len(df)} articles, {df['pub_date'].min()} to {df['pub_date'].max()}; latest: {df['title'].iloc[0]}")

save_csv(df, PREFIX + 'lds-newsroom-rss-' + RUN_LABEL + '.csv')
