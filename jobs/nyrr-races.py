import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

from common import RUN_DATETIME, RUN_LABEL, get, save_csv

# NYRR's events calendar embeds its complete event list in Astro's serialized props
URL = 'https://www.nyrr.org/run/events-calendar'
NY = ZoneInfo('America/New_York')
CATEGORIES = {'races', 'virtual', 'youth'}

# a Session keeps cookies across NYRR's waiting-room redirects
with requests.Session() as session:
    page = get(URL, session=session, timeout=60, name='nyrr events calendar').text

calendar = BeautifulSoup(page, 'lxml').select_one('astro-island[component-export="DynamicScheduleEventCards"]')
if calendar is None or not calendar.get('props'):
    raise RuntimeError('NYRR calendar event data is missing; check for a waiting room or page redesign')
props = json.loads(calendar['props'])

# Astro encodes each value as [type, value]; events is an array of [type, object]
def value(obj, name):
    field = obj.get(name)
    if not isinstance(field, list) or len(field) < 2 or field[1] is None:
        return None
    return str(field[1])

events = props.get('events')
if not isinstance(events, list) or len(events) < 2:
    raise RuntimeError('NYRR calendar events are missing')

rows = []
for entry in events[1]:
    event = entry[1]
    if value(event, 'category') not in CATEGORIES:
        continue
    start = value(event, 'startDateTime')
    start = datetime.strptime(start, '%Y-%m-%dT%H:%M:%S.%fZ' if '.' in start else '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=ZoneInfo('UTC')).astimezone(NY) if start else None
    location = (event.get('location') or [None, {}])[1] or {}
    rows.append({
        'dates': start.strftime('%Y-%m-%d') if start else None,
        'times': start.strftime('%H:%M') if start else None,
        'titles': value(event, 'title'),
        'statuses': value(event, 'registrationStatus'),
        'locations': value(location, 'venue'),
    })
df = pd.DataFrame(rows, columns=['dates', 'times', 'titles', 'statuses', 'locations'])
if df.empty or df['titles'].isna().any() or df['dates'].isna().any():
    raise RuntimeError('NYRR calendar returned no races or invalid race data; refusing to upload')
df['row_number'] = range(1, len(df) + 1)
df['extracted_at'] = RUN_DATETIME

# the "2024" in the key is historical; kept so new files line up with old ones
save_csv(df, 'nyrr/nyrr-2024-races-' + RUN_LABEL + '.csv')
