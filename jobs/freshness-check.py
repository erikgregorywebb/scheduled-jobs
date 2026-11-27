import os
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import yaml

from common import RUN_LABEL, RUN_TIME, list_objects, save_csv

# Checks every output listed in jobs.yml against the bucket, and fails (so
# GitHub emails a failed-run notice) when:
#   stale    the newest file is older than the job's frequency allows
#            (Hourly 4h, Every 4 Hours 9h, Daily 26h, or the output's max_age_hours)
#   missing  no file has ever been written under the stem
#   small    the newest file is under min_size_ratio (default 0.5) of the median
#            size of the 7 files before it, e.g. a page that came back empty
# Writes one row per output to data-pipeline-logs/freshness/, problems or not.

ROOT = Path(__file__).resolve().parent.parent
MAX_AGE_HOURS = {'Hourly': 4, 'Every 4 Hours': 9, 'Daily': 26}
DEFAULT_MIN_SIZE_RATIO = 0.5
SIZE_HISTORY = 7      # earlier files to take the median size of
MIN_SIZE_HISTORY = 3  # skip the size check with fewer than this many
SELF = 'freshness-check'
# a key's stem is everything before its trailing timestamp and extension
TIMESTAMP = re.compile(r'\d{4}-\d{2}-\d{2}(-\d{2}-\d{2}-\d{2})?(\.[a-z.]+)$')

with open(ROOT / 'jobs.yml') as f:
    manifest = yaml.safe_load(f)

# one listing of the bucket, grouped by stem
by_stem = defaultdict(list)
for o in list_objects(''):
    if TIMESTAMP.search(o['Key']):
        by_stem[TIMESTAMP.sub('', o['Key'])].append(o)
if not by_stem:
    raise RuntimeError('Listing the bucket returned no timestamped files')

rows = []
for name, job in manifest['jobs'].items():
    if name == SELF:
        continue
    for output in job['outputs']:
        spec = output if isinstance(output, dict) else {'stem': output}
        stem = spec['stem']
        max_age = spec.get('max_age_hours', MAX_AGE_HOURS[job['frequency']])
        min_ratio = spec.get('min_size_ratio', DEFAULT_MIN_SIZE_RATIO)
        row = {'job': name, 'workflow': job['workflow'], 'stem': stem, 'max_age_hours': max_age}
        files = sorted(by_stem.get(stem, []), key=lambda o: o['LastModified'])
        if not files:
            rows.append({**row, 'status': 'missing'})
            continue
        newest = files[-1]
        age = (RUN_TIME - newest['LastModified']).total_seconds() / 3600
        row.update({
            'newest_key': newest['Key'],
            'newest_at_utc': newest['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
            'age_hours': round(age, 1),
            'newest_size': newest['Size'],
            'file_count': len(files),
        })
        earlier = [o['Size'] for o in files[-1 - SIZE_HISTORY:-1]]
        if len(earlier) >= MIN_SIZE_HISTORY and statistics.median(earlier) > 0:
            row['median_earlier_size'] = round(statistics.median(earlier))  # a whole number of bytes, for the Int64 column
            row['size_ratio'] = round(newest['Size'] / row['median_earlier_size'], 3)
        if age > max_age:
            row['status'] = 'stale'
        elif min_ratio is not None and row.get('size_ratio') is not None and row['size_ratio'] < min_ratio:
            row['status'] = 'small'
        else:
            row['status'] = 'ok'
        rows.append(row)

report = pd.DataFrame(rows, columns=['job', 'workflow', 'stem', 'status', 'newest_key', 'newest_at_utc',
                                     'age_hours', 'max_age_hours', 'newest_size', 'median_earlier_size',
                                     'size_ratio', 'file_count'])
for col in ['newest_size', 'median_earlier_size', 'file_count']:
    report[col] = report[col].astype('Int64')
report['checked_at_utc'] = RUN_TIME.strftime('%Y-%m-%d %H:%M:%S')
problems = report[report['status'] != 'ok']
print(f'{len(report)} outputs checked: ' + ', '.join(f'{n} {s}' for s, n in report['status'].value_counts().items()))

save_csv(report, 'data-pipeline-logs/freshness/freshness-' + RUN_LABEL + '.csv')

for p in problems.itertuples():
    detail = {
        'stale': f'newest file is {p.age_hours}h old (limit {p.max_age_hours}h): {p.newest_key}',
        'missing': 'no files under this stem',
        'small': f'newest file is {p.size_ratio:.0%} of the usual size: {p.newest_key}',
    }[p.status]
    print(f'::error title={p.job} {p.status}::{p.stem}: {detail}')

summary = os.environ.get('GITHUB_STEP_SUMMARY')
if summary:
    with open(summary, 'a') as f:
        if problems.empty:
            f.write(f'All {len(report)} job outputs are fresh.\n')
        else:
            f.write(f'## {len(problems)} of {len(report)} job outputs need a look\n\n')
            f.write('| job | workflow | stem | status | age (h) | limit (h) | size ratio |\n|---|---|---|---|---|---|---|\n')
            for p in problems.itertuples():
                f.write(f'| {p.job} | {p.workflow} | `{p.stem}` | {p.status} | {p.age_hours} | {p.max_age_hours} | {p.size_ratio} |\n')

if not problems.empty:
    sys.exit(f'{len(problems)} job outputs are stale, missing or unusually small')
