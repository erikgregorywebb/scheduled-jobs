import pandas as pd

from common import RUN_DATETIME, RUN_LABEL, get_json, save_csv

# Collects every meetinghouse and the units (wards, stakes, FamilySearch centers, ...) that meet there
# from the Meetinghouse Locator at https://maps.churchofjesuschrist.org/.
#
# Replaces two earlier R scripts (see git history); the last one's endpoint and session token
# stopped working in January 2025.
# The locator's "identify" endpoint returns the nearest N meetinghouses to a point, with full details;
# asking for more than exist returns all of them (~19,800 in Nov 2026) in one ~60-second, ~16 MB call.
# No token or cookies are needed.
#
# Output (one pair of files per run; the v3/ prefix keeps this layout apart from the older R scripts' files):
#   lds-meetinghouses/v3/meetinghouses-<datetime>.csv  one row per meetinghouse
#   lds-meetinghouses/v3/associations-<datetime>.csv   one row per unit meeting at a meetinghouse

# well above the current total; the script stops if the API reports more than this
MAX_LOCATIONS = 50000

url = 'https://maps.churchofjesuschrist.org/api/maps-proxy/v2/locations/identify'
params = {
    'layers': 'MEETINGHOUSE',
    'filters': '',
    'associated': 'ALL',
    'coordinates': '-111.891,40.7608',  # any point works; this is Salt Lake City
    'nearest': MAX_LOCATIONS,
}
headers = {
    'Accept': 'application/json',
    'Accept-Language': 'en',
    'Referer': 'https://maps.churchofjesuschrist.org/',
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/129.0.0.0 Safari/537.36'
    ),
}

def get_locations():
    return get_json(url, params=params, headers=headers, timeout=300, delays=(30, 60), name='Meetinghouse')

def get(d, *keys):
    # walk nested dicts, returning None if any key is missing
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d

locations = get_locations()
if not locations:
    raise SystemExit('No meetinghouses returned')
total = get(locations[0], 'match', 'total')
print(f'Fetched {len(locations)} of {total} meetinghouses')
if total is not None and total > len(locations):
    raise SystemExit(f'API reports {total} meetinghouses but returned {len(locations)}; raise MAX_LOCATIONS')

meetinghouses = []
associations = []
for m in locations:
    coords = m.get('coordinates') or []
    plus_codes = [g.get('code') for g in m.get('geocodes') or [] if g.get('type') == 'PLUS_CODE']
    units = m.get('associated') or []
    meetinghouses.append({
        'id': m.get('id'),
        'name': m.get('name'),
        'name_display': m.get('nameDisplay'),
        'type': m.get('type'),
        'facility_id': get(m, 'identifiers', 'facilityId'),
        'structure_id': get(m, 'identifiers', 'structureId'),
        'property_id': get(m, 'identifiers', 'propertyId'),
        'street_1': get(m, 'address', 'street1'),
        'street_2': get(m, 'address', 'street2'),
        'city': get(m, 'address', 'city'),
        'county': get(m, 'address', 'county'),
        'state': get(m, 'address', 'state'),
        'state_code': get(m, 'address', 'stateCode'),
        'postal_code': get(m, 'address', 'postalCode'),
        'country': get(m, 'address', 'country'),
        'country_code_2': get(m, 'address', 'countryCode2'),
        'country_code_3': get(m, 'address', 'countryCode3'),
        'formatted_address': get(m, 'address', 'formatted'),
        'longitude': coords[0] if len(coords) >= 2 else None,
        'latitude': coords[1] if len(coords) >= 2 else None,
        'plus_code': plus_codes[0] if plus_codes else None,
        'hours': get(m, 'hours', 'code'),
        'time_zone': get(m, 'timeZone', 'id'),
        'website': m.get('website'),
        'unit_count': len(units),
        'scraped_at': RUN_DATETIME,
    })
    for a in units:
        associations.append({
            'meetinghouse_id': m.get('id'),
            'id': a.get('id'),
            'type': a.get('type'),
            'name': a.get('name'),
            'name_display': a.get('nameDisplay'),
            'type_display': a.get('typeDisplay'),
            'sub_type': a.get('subType'),
            'sub_type_display': a.get('subTypeDisplay'),
            'unit_number': get(a, 'identifiers', 'unitNumber'),
            'org_id': get(a, 'identifiers', 'orgId'),
            'unit_uuid': get(a, 'identifiers', 'unitUuid'),
            'facility_id': get(a, 'identifiers', 'facilityId'),
            'language_code': get(a, 'language', 'code'),
            'language_display': get(a, 'language', 'display'),
            'hours': get(a, 'hours', 'code'),
            'scraped_at': RUN_DATETIME,
        })

meetinghouses = pd.DataFrame(meetinghouses)
# keep id columns as whole numbers even where some rows are missing them
associations = pd.DataFrame(associations).astype({'unit_number': 'Int64', 'org_id': 'Int64'})
print(f'Parsed {len(meetinghouses)} meetinghouses and {len(associations)} associations')

save_csv(meetinghouses, 'lds-meetinghouses/v3/meetinghouses-' + RUN_LABEL + '.csv')
save_csv(associations, 'lds-meetinghouses/v3/associations-' + RUN_LABEL + '.csv')
