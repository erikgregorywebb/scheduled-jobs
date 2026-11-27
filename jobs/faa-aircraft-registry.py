from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import zipfile
from io import BytesIO

from common import DRY_RUN, get, object_exists, save_bytes

# Saves the FAA's full aircraft registry database, unchanged, to S3.
# The FAA overwrites this file every weekday (around 06:00 UTC); MASTER.txt only
# holds each aircraft's current owner, so these daily copies are the only record
# of ownership, address, and status history. See the FAA's field definitions:
# https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download
# robots.txt on registry.faa.gov only disallows /aircraftinquiry/*.asp (checked 2026-11-27).
# The registry includes names and addresses of individual owners; keep the bucket private.

URL = 'https://registry.faa.gov/database/ReleasableAircraft.zip'
# the server refuses requests without a browser-like User-Agent
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; scheduled-jobs data collector)'}
PREFIX = 'faa-aircraft-registry/'
EXPECTED_FILES = {'MASTER.txt', 'DEREG.txt', 'ACFTREF.txt', 'ENGINE.txt'}

# download, retrying transient failures
response = get(URL, headers=HEADERS, timeout=300, delays=(30, 60), name='Registry zip')

content = response.content
print(f'Downloaded {len(content):,} bytes')

# make sure we got the real archive, not an error page, before saving it
with zipfile.ZipFile(BytesIO(content)) as archive:
    names = set(archive.namelist())
    missing = EXPECTED_FILES - names
    if missing:
        raise RuntimeError(f'Registry zip is missing {sorted(missing)}; got {sorted(names)}')
    bad = archive.testzip()
    if bad:
        raise RuntimeError(f'Registry zip has a corrupt member: {bad}')

# name the file after when the FAA last updated it, so a day with no new
# release (weekends, holidays) is skipped instead of saving a duplicate 73 MB copy
last_modified = response.headers.get('Last-Modified')
if last_modified:
    source_time = parsedate_to_datetime(last_modified).astimezone(timezone.utc)
else:
    source_time = datetime.now(timezone.utc)
    print('No Last-Modified header; naming the file after the download time')
file_path = PREFIX + 'faa-aircraft-registry-' + source_time.strftime('%Y-%m-%d-%H-%M-%S') + '.zip'

# a dry run (DRY_RUN=1) skips the S3 check and writes the zip under ./out/
if not DRY_RUN and object_exists(file_path):
    print(f'{file_path} already saved; the FAA has not published a new release since. Skipping.')
else:
    print(f'Zip holds {sorted(names)}')
    save_bytes(
        content, file_path,
        content_type='application/zip',
        metadata={
            'source-url': URL,
            'source-last-modified': last_modified or '',
            'downloaded-at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        },
    )
