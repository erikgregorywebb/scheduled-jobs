"""Shared helpers for the scripts in jobs/.

Import with `from common import ...`. Python puts a script's own folder on
sys.path, so `python jobs/<script>.py` finds this file from any directory.

    RUN_TIME / RUN_DATETIME / RUN_LABEL  one UTC timestamp per run
    get / get_json / post / request      HTTP with timeouts and retries
    save_csv / save_bytes                write to S3 (or locally, with DRY_RUN=1)
    s3 / list_objects / latest_object / object_exists   read the bucket

Set DRY_RUN=1 to write files under ./out/ (or $DRY_RUN_DIR) instead of S3;
nothing is uploaded, and no AWS credentials are needed unless a script also
reads from the bucket.
"""
import hashlib
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BUCKET = 'egw-data-dumps'

# One timestamp for the whole run, in UTC, so a file's name and its datetime
# column always agree and don't depend on the machine's local time zone.
RUN_TIME = datetime.now(timezone.utc)
RUN_DATETIME = RUN_TIME.strftime('%Y-%m-%d %H:%M:%S')  # value for a `datetime` column
RUN_LABEL = RUN_TIME.strftime('%Y-%m-%d-%H-%M-%S')     # timestamp in S3 keys

DRY_RUN = os.environ.get('DRY_RUN', '').lower() not in ('', '0', 'false', 'no')
DRY_RUN_DIR = Path(os.environ.get('DRY_RUN_DIR', 'out'))


# ---------------------------------------------------------------- HTTP

RETRY_DELAYS = (10, 30, 60)  # seconds to wait before each retry
TRANSIENT_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


def request(method, url, *, delays=RETRY_DELAYS, timeout=30, session=None, name=None, **kwargs):
    """Send a request, retrying connection errors, timeouts, 5xx and 429 after
    each of `delays` seconds. Any other 4xx raises straight away (retrying a bad
    request won't fix it). Returns the response; raises once retries run out.
    `session` may be a requests.Session; other kwargs go to requests."""
    name = name or url
    sender = session or requests
    for attempt in range(len(delays) + 1):
        try:
            response = sender.request(method, url, timeout=timeout, **kwargs)
        except TRANSIENT_ERRORS as e:
            error = f'{type(e).__name__}: {e}'
        else:
            if response.status_code < 500 and response.status_code != 429:
                response.raise_for_status()
                return response
            error = f'HTTP {response.status_code}'
        if attempt == len(delays):
            raise RuntimeError(f'{name} request failed after {attempt + 1} attempts: {error}')
        print(f'{name} attempt {attempt + 1} failed ({error}), retrying in {delays[attempt]}s')
        time.sleep(delays[attempt])


def get(url, **kwargs):
    return request('GET', url, **kwargs)


def post(url, **kwargs):
    return request('POST', url, **kwargs)


def get_json(url, **kwargs):
    return get(url, **kwargs).json()


# ---------------------------------------------------------------- S3

_s3 = None


def s3():
    """A boto3 S3 client. Credentials come from AWS_ACCESS_KEY_ID and
    AWS_SECRET_ACCESS_KEY in the environment."""
    global _s3
    if _s3 is None:
        import boto3
        _s3 = boto3.client('s3')
    return _s3


def save_bytes(body, key, *, content_type=None, metadata=None, label=None):
    """Write `body` (bytes or str) to `key` in the bucket, or under DRY_RUN_DIR
    when DRY_RUN is set."""
    if isinstance(body, str):
        body = body.encode('utf-8')
    label = label or f'{len(body):,} bytes'
    if DRY_RUN:
        path = DRY_RUN_DIR / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        print(f'DRY RUN: wrote {label} to {path} (would be s3://{BUCKET}/{key})')
        return
    extra = {}
    if content_type:
        extra['ContentType'] = content_type
    if metadata:
        extra['Metadata'] = metadata
    s3().put_object(Bucket=BUCKET, Key=key, Body=body, **extra)
    print(f'Copy {label} to S3 Bucket {BUCKET} at {key}, Done!')


def save_csv(df, key):
    """Write a DataFrame to `key` as CSV with a header and no index."""
    save_bytes(df.to_csv(index=False), key, content_type='text/csv', label=f'{df.shape[0]} rows')


def list_objects(prefix):
    """Every object under `prefix`, as boto3 dicts (Key, LastModified, Size, ETag)."""
    paginator = s3().get_paginator('list_objects_v2')
    return [o for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix) for o in page.get('Contents', [])]


def latest_object(prefix):
    """The most recently written object under `prefix`, or None."""
    objects = [o for o in list_objects(prefix) if not o['Key'].endswith('/')]
    return max(objects, key=lambda o: o['LastModified']) if objects else None


def object_exists(key):
    from botocore.exceptions import ClientError
    try:
        s3().head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] in ('404', 'NoSuchKey', 'NotFound'):
            return False
        raise


def same_as_latest(body, prefix):
    """True if `body` is byte-for-byte the newest object under `prefix`, using
    its ETag (the MD5 of a single-part upload, which is how save_bytes writes)."""
    if isinstance(body, str):
        body = body.encode('utf-8')
    latest = latest_object(prefix)
    return latest is not None and latest['ETag'].strip('"') == hashlib.md5(body).hexdigest()
