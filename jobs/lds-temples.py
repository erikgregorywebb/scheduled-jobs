import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import json
import re

from common import RUN_DATETIME, RUN_LABEL, get as http_get, save_csv

# Collects every LDS temple (operating, under renovation, opening soon, under construction and announced)
# from three public sources on churchofjesuschrist.org. No token or cookies are needed.
#
#   1. The temple list page, https://www.churchofjesuschrist.org/temples/list, embeds every temple
#      (~385 in Nov 2026) with its status and location in its __NEXT_DATA__ JSON.
#   2. The Meetinghouse Locator's TEMPLE layer (the same endpoint lds-meetinghouses.py uses) adds the
#      full address, coordinates, phone, hours and upcoming closures, but only for operating temples.
#   3. Each temple's details page adds its milestones (announced, groundbreaking, dedicated, rededicated).
#
# Output (one set of files per run):
#   lds-temples/temples-<datetime>.csv     one row per temple
#   lds-temples/milestones-<datetime>.csv  one row per temple milestone
#   lds-temples/closures-<datetime>.csv    one row per upcoming closure of an operating temple

LIST_URL = 'https://www.churchofjesuschrist.org/temples/list'
DETAILS_URL = 'https://www.churchofjesuschrist.org/temples/details/{}'
LOCATOR_URL = 'https://maps.churchofjesuschrist.org/api/maps-proxy/v2/locations/identify'
headers = {
    'Accept-Language': 'en',
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/129.0.0.0 Safari/537.36'
    ),
}

# the script stops if more than this share of details pages fail, rather than upload a thin milestones file
MAX_DETAIL_FAILURE_SHARE = 0.05
# details pages fetched at once; enough to finish in a few minutes without hammering the site
DETAIL_WORKERS = 4

# returns None when the request still fails after retries (or hits a non-retryable 4xx),
# so callers can decide whether one failure is fatal
def fetch(url, params=None, extra_headers=None, attempts=3, timeout=60):
    try:
        return http_get(url, params=params, headers={**headers, **(extra_headers or {})}, timeout=timeout,
                        delays=[10 * a for a in range(1, attempts)])
    except Exception as e:
        print(f'{url}: {e}')
        return None

def next_data(html):
    # the page props Next.js embeds in every page
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    return json.loads(m.group(1))['props']['pageProps'] if m else None

def get(d, *keys):
    # walk nested dicts, returning None if any key is missing
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d

def clean(s):
    # the list page pads some location parts with a leading space and uses '' for unknown
    s = (s or '').strip()
    return s or None

def to_date(s):
    d = pd.to_datetime(s, format='%B %d, %Y', errors='coerce')
    return None if pd.isna(d) else d.strftime('%Y-%m-%d')

# 1. temple list
r = fetch(LIST_URL, params={'lang': 'eng'})
if r is None:
    raise SystemExit('Temple list request failed after 3 attempts')
temple_list = (next_data(r.text) or {}).get('templeList')
if not temple_list:
    raise SystemExit('No templeList found in the temple list page')
print(f'Fetched {len(temple_list)} temples from the temple list')

# 2. locator details for operating temples, keyed by org id (the list's templeOrgId)
r = fetch(
    LOCATOR_URL,
    params={'layers': 'TEMPLE', 'filters': '', 'associated': 'ALL', 'coordinates': '-111.891,40.7608', 'nearest': 5000},
    extra_headers={'Accept': 'application/json', 'Referer': 'https://maps.churchofjesuschrist.org/'},
    timeout=120,
)
if r is None:
    raise SystemExit('Temple locator request failed after 3 attempts')
locator = {get(t, 'identifiers', 'orgId'): t for t in r.json()}
print(f'Fetched {len(locator)} temples from the locator')

# 3. milestones from each temple's details page; pages can take seconds each, so fetch a few at a time
def get_details(slug):
    r = fetch(DETAILS_URL.format(slug), params={'lang': 'eng'}, attempts=2)
    return next_data(r.text) if r is not None else None

with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as pool:
    details = list(pool.map(get_details, [t.get('templeNameId') for t in temple_list]))

milestones = []
failed = []
for t, props in zip(temple_list, details):
    slug = t.get('templeNameId')
    if props is None:
        failed.append(slug)
        continue
    for m in get(props, 'historicalMilestones', 'dates') or []:
        milestones.append({
            'temple_org_id': t.get('templeOrgId'),
            'temple_name_id': slug,
            'temple_name': t.get('name'),
            'milestone': m.get('milestone'),
            'date_text': m.get('date'),
            'date': m.get('dedicationDate') or to_date(m.get('date')),
            'scraped_at': RUN_DATETIME,
        })
print(f'Fetched {len(temple_list) - len(failed)} details pages; {len(failed)} failed: {failed}')
if len(failed) > MAX_DETAIL_FAILURE_SHARE * len(temple_list):
    raise SystemExit(f'{len(failed)} of {len(temple_list)} details pages failed')

# first date of each milestone per temple, for the temples table (rededications can repeat)
first_milestone = {}
for m in milestones:
    key = (m['temple_org_id'], m['milestone'])
    if m['date'] and (key not in first_milestone or m['date'] < first_milestone[key]):
        first_milestone[key] = m['date']

temples = []
closures = []
for t in temple_list:
    org_id = t.get('templeOrgId')
    loc = locator.get(org_id) or {}
    coords = loc.get('coordinates') or []
    phones = [p.get('number') for p in loc.get('phones') or []]
    emails = [e.get('email') for e in loc.get('emails') or []]
    temples.append({
        'temple_org_id': org_id,
        'temple_name_id': t.get('templeNameId'),
        'name': t.get('name'),
        'status': t.get('status'),
        'location': clean(t.get('location')),
        'city': clean(t.get('city')),
        'state_region': clean(t.get('stateRegion')),
        'country': clean(t.get('country')),
        'list_date': clean(t.get('date')),  # dedication date for operating temples, else the status
        'announced_date': first_milestone.get((org_id, 'Announced')),
        'groundbreaking_date': first_milestone.get((org_id, 'Groundbreaking')),
        'dedicated_date': first_milestone.get((org_id, 'Dedicated')),
        'rededicated_date': first_milestone.get((org_id, 'Rededicated')),
        'in_locator': bool(loc),
        'unit_number': get(loc, 'identifiers', 'unitNumber'),
        'facility_id': get(loc, 'identifiers', 'facilityId'),
        'property_id': get(loc, 'identifiers', 'propertyId'),
        'street_1': get(loc, 'address', 'street1'),
        'street_2': get(loc, 'address', 'street2'),
        'address_city': get(loc, 'address', 'city'),
        'county': get(loc, 'address', 'county'),
        'state': get(loc, 'address', 'state'),
        'state_code': get(loc, 'address', 'stateCode'),
        'postal_code': get(loc, 'address', 'postalCode'),
        'address_country': get(loc, 'address', 'country'),
        'country_code_2': get(loc, 'address', 'countryCode2'),
        'country_code_3': get(loc, 'address', 'countryCode3'),
        'formatted_address': get(loc, 'address', 'formatted'),
        'longitude': coords[0] if len(coords) >= 2 else None,
        'latitude': coords[1] if len(coords) >= 2 else None,
        'phone': phones[0] if phones else None,
        'email': emails[0] if emails else None,
        'hours': get(loc, 'hours', 'code'),
        'time_zone': get(loc, 'timeZone', 'id'),
        'website': loc.get('website') or f'https://www.churchofjesuschrist.org/temples/details/{t.get("templeNameId")}',
        'scraped_at': RUN_DATETIME,
    })
    for event in loc.get('events') or []:
        for d in event.get('dates') or []:
            closures.append({
                'temple_org_id': org_id,
                'temple_name_id': t.get('templeNameId'),
                'temple_name': t.get('name'),
                'event_type': event.get('type'),
                'start_date': get(d, 'start', 'code'),
                'end_date': get(d, 'end', 'code'),
                'display': d.get('display'),
                'scraped_at': RUN_DATETIME,
            })

temples = pd.DataFrame(temples).astype({'unit_number': 'Int64', 'property_id': 'Int64'})
milestones = pd.DataFrame(milestones)
closures = pd.DataFrame(closures)
print(f'Parsed {len(temples)} temples ({temples["status"].value_counts().to_dict()}), '
      f'{len(milestones)} milestones and {len(closures)} closures')

save_csv(temples, 'lds-temples/temples-' + RUN_LABEL + '.csv')
save_csv(milestones, 'lds-temples/milestones-' + RUN_LABEL + '.csv')
save_csv(closures, 'lds-temples/closures-' + RUN_LABEL + '.csv')
