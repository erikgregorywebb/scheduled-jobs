import re
import xml.etree.ElementTree as ET
from datetime import datetime

import pandas as pd

from common import RUN_DATETIME, RUN_LABEL, get, save_csv

# the State Department's official travel advisory RSS feed
# (the advisories HTML page is behind a Cloudflare check that blocks scripts)
URL = 'https://travel.state.gov/_res/rss/TAsTWs.xml'
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; scheduled-jobs/1.0)'}

# feed names mapped back to the names used in files saved from the old HTML table,
# so destinations line up across the history. Not mapped: Israel, Gaza and The West
# Bank (one combined entry before) and Saba and Sint Eustatius (listed differently before).
NAME_MAP = {
    'Burma': 'Burma (Myanmar)',
    'Côte d’Ivoire': "Côte d'Ivoire (Ivory-Coast)",
    'Democratic Republic of the Congo': 'Democratic Republic of the Congo (D.R.C.)',
    'Eswatini': 'Eswatini (Swaziland)',
    'French Saint Martin': 'Saint Martin (French West Indies)',
    'Guadeloupe': 'Guadeloupe (French West Indies)',
    'Martinique': 'Martinique (French West Indies)',
    'Naoero': 'Nauru',
    'North Korea': "North Korea (Democratic People's Republic of Korea)",
    'North Macedonia': 'Republic of North Macedonia',
    'Republic of Congo': 'Republic of the Congo',
    'Saint Barthelemy': 'Saint Barthélemy (French West Indies)',
    'São Tomé and Príncipe': 'Sao Tome and Principe',
    'The Bahamas': 'Bahamas',
    'United Kingdom': 'United Kingdom of Great Britain and Northern Ireland',
}

response = get(URL, headers=HEADERS, timeout=60, name='travel advisories feed')
items = ET.fromstring(response.content).findall('.//item')
if not items:
    raise RuntimeError('Travel advisory feed returned no items')

def sentence_case(s):
    return s[:1].upper() + s[1:].lower()

def to_date(pub_date):
    # e.g. "Tue, 24 Nov 2026": drop the weekday, keep day month year
    if not pub_date:
        return None
    day_month_year = ' '.join(re.sub(r'^[A-Za-z]+,\s*', '', pub_date).split()[:3])
    return datetime.strptime(day_month_year, '%d %b %Y').strftime('%Y-%m-%d')

rows = []
for item in items:
    # titles look like "Afghanistan - Level 4: Do Not Travel"
    title = item.findtext('title') or ''
    advisory = re.sub(r'\s+-\s+Level\s+\d.*$', '', title)
    advisory = re.sub(r'\s+Travel Advisory$', '', advisory).strip()
    advisory = NAME_MAP.get(advisory, advisory)
    level_number = re.search(r'Level\s+\d', title)
    level_text = re.search(r'(?<=Level\s\d:).*$', title)
    # sentence case, e.g. "Level 4: Do not travel", to match files saved from the old HTML table
    level = (level_number.group(0) if level_number else 'NA') + ': ' + (sentence_case(level_text.group(0).strip()) if level_text else 'NA')
    pub_date = item.findtext('pubDate')
    date_updated = to_date(pub_date)
    rows.append({
        'advisory': advisory,
        'level': level,
        'date_updated': date_updated,
        'url': item.findtext('link'),
        'scraped_at': RUN_DATETIME,
    })
df = pd.DataFrame(rows)

save_csv(df, 'travel-advisories/travel-advisories-' + RUN_LABEL + '.csv')
