import requests
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import zipfile
import boto3
from botocore.exceptions import ClientError
from io import BytesIO
import time
import os

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
BUCKET = 'egw-data-dumps'
PREFIX = 'faa-aircraft-registry/'
EXPECTED_FILES = {'MASTER.txt', 'DEREG.txt', 'ACFTREF.txt', 'ENGINE.txt'}

# download, retrying transient failures
for attempt in range(3):
    try:
        response = requests.get(URL, headers=HEADERS, timeout=300)
        response.raise_for_status()
        break
    except requests.RequestException as e:
        if attempt == 2:
            raise
        print(f'Attempt {attempt + 1} failed ({e}), retrying')
        time.sleep(30 * (attempt + 1))

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

if os.environ.get('DRY_RUN'):
    # local check without touching S3: DRY_RUN=1 python jobs/faa-aircraft-registry.py
    print(f'Dry run: would save {len(content):,} bytes to {file_path}; zip holds {sorted(names)}')
    raise SystemExit(0)

# get environmental variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

try:
    s3.head_object(Bucket=BUCKET, Key=file_path)
    print(f'{file_path} already saved; the FAA has not published a new release since. Skipping.')
except ClientError as e:
    if e.response['Error']['Code'] not in ('404', 'NoSuchKey', 'NotFound'):
        raise
    s3.upload_fileobj(
        BytesIO(content), BUCKET, file_path,
        ExtraArgs={
            'ContentType': 'application/zip',
            'Metadata': {
                'source-url': URL,
                'source-last-modified': last_modified or '',
                'downloaded-at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            },
        },
    )
    print(f'Copy {len(content):,} bytes to S3 Bucket {BUCKET} at {file_path}, Done!')
