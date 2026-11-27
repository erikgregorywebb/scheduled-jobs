import re

import pandas as pd

from common import BUCKET, RUN_LABEL, list_objects, s3, save_csv

# Logs what changed in the bucket since the last run, instead of a full listing
# every day (full listings grew with the bucket: ~4 MB a day by late 2026).
#
#   data-pipeline-logs/s3-changes/s3-changes-<timestamp>.csv
#       one row per object added, modified or removed since the previous run;
#       written every run, header-only when nothing changed
#   data-pipeline-logs/s3-snapshot-state.csv
#       the full listing as of this run, overwritten each time; it's what the
#       next run diffs against
#
# The old daily full listings (data-pipeline-logs/s3-snapshot-<timestamp>.csv,
# up to 2026-11-27) are left in place: the newest one plus the change files
# rebuild the bucket's contents on any later date.

STATE_KEY = 'data-pipeline-logs/s3-snapshot-state.csv'
CHANGES_PREFIX = 'data-pipeline-logs/s3-changes/s3-changes-'
OLD_SNAPSHOT = re.compile(r'^data-pipeline-logs/s3-snapshot-\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.csv$')
COLUMNS = ['key', 'last_modified', 'size', 'e_tag']


def current_listing():
    rows = [{
        'key': o['Key'],
        'last_modified': o['LastModified'],
        'size': o['Size'],
        'e_tag': o['ETag'],
    } for o in list_objects('') if o['Key'] != STATE_KEY]
    return normalize(pd.DataFrame(rows, columns=COLUMNS))


def normalize(df):
    # same text form whether a timestamp came from boto3 or a saved CSV,
    # e.g. '2026-11-27 02:09:24+00:00'
    df = df.copy()
    df['last_modified'] = pd.to_datetime(df['last_modified'], utc=True).dt.strftime('%Y-%m-%d %H:%M:%S+00:00')
    df['size'] = df['size'].astype('int64')
    return df


def read_csv_key(key):
    body = s3().get_object(Bucket=BUCKET, Key=key)['Body']
    return pd.read_csv(body, dtype={'key': str, 'e_tag': str}, keep_default_na=False)


def previous_listing():
    """The listing from the last run: the state file, or on the first run of
    this version, the newest of the old full daily snapshots."""
    try:
        return normalize(read_csv_key(STATE_KEY)), STATE_KEY
    except s3().exceptions.NoSuchKey:
        pass
    old = [o for o in list_objects('data-pipeline-logs/s3-snapshot-') if OLD_SNAPSHOT.match(o['Key'])]
    if not old:
        raise RuntimeError(f'No {STATE_KEY} and no old full snapshot to start from')
    newest = max(old, key=lambda o: o['LastModified'])['Key']
    return normalize(read_csv_key(newest)), newest


now = current_listing()
if now.empty:
    raise RuntimeError(f'Listing {BUCKET} returned no objects')
before, source = previous_listing()
print(f'{len(now):,} objects now; diffing against {len(before):,} in {source}')

merged = now.merge(before, on='key', how='outer', suffixes=('', '_previous'), indicator=True)
added = merged['_merge'] == 'left_only'
removed = merged['_merge'] == 'right_only'
both = merged['_merge'] == 'both'
modified = both & ((merged['e_tag'] != merged['e_tag_previous'])
                   | (merged['size'] != merged['size_previous'])
                   | (merged['last_modified'] != merged['last_modified_previous']))

merged['change'] = None
merged.loc[added, 'change'] = 'added'
merged.loc[removed, 'change'] = 'removed'
merged.loc[modified, 'change'] = 'modified'
changes = merged[merged['change'].notna()]
changes = changes[['change', 'key', 'last_modified', 'size', 'e_tag',
                   'last_modified_previous', 'size_previous', 'e_tag_previous']]
for col in ['size', 'size_previous']:
    changes[col] = changes[col].astype('Int64')
changes = changes.sort_values(['change', 'key']).reset_index(drop=True)

print(f"{int(added.sum())} added, {int(modified.sum())} modified, {int(removed.sum())} removed")

# changes first: if the state write then fails, the next run diffs against the
# older state and reports these changes again rather than losing them
save_csv(changes, CHANGES_PREFIX + RUN_LABEL + '.csv')
save_csv(now, STATE_KEY)
