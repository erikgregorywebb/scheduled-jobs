from email.utils import parsedate_to_datetime

from common import RUN_LABEL, get, latest_object, request, same_as_latest, save_bytes

# Zillow's zip-code-level ZHVI (typical home value, all homes, smoothed and
# seasonally adjusted), saved exactly as Zillow serves it (~120 MB)
URL = 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1693977204'
PREFIX = 'zillow/'

# Zillow republishes this file about once a month. Compare when Zillow last
# changed it with our newest saved copy, and skip the download if that copy is
# newer. When either date can't be read, download and compare contents instead.
try:
    head = request('HEAD', URL, timeout=60, name='zillow HEAD')
    source_modified = parsedate_to_datetime(head.headers['Last-Modified'])
except Exception as e:
    print(f'could not read Zillow Last-Modified: {e}')
    source_modified = None
latest = latest_object(PREFIX)
latest_saved = latest['LastModified'] if latest else None
print(f'Zillow last modified: {source_modified} | newest saved copy: {latest_saved}')

if source_modified and latest_saved and latest_saved > source_modified:
    print('job run: zillow file unchanged since last saved copy, skipping upload')
    raise SystemExit(0)

body = get(URL, timeout=300, name='zillow').content
if not body.startswith(b'RegionID') or body.count(b'\n') < 1000:
    raise RuntimeError(f'Zillow file looks wrong ({len(body):,} bytes, starts {body[:80]!r})')
if same_as_latest(body, PREFIX):
    print('job run: zillow file identical to newest saved copy, skipping upload')
    raise SystemExit(0)

save_bytes(body, PREFIX + 'zhvi-all-homes-zip-code-' + RUN_LABEL + '.csv', content_type='text/csv')
