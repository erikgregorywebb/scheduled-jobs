import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

import pandas as pd

from common import RUN_LABEL, RUN_TIME, list_objects, save_csv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'tools'))
from run_log import PREFIX, manifest, period_start, read_records, workflow_frequency  # noqa: E402

# Rolls up the run records tools/run_log.py writes for every workflow run into
# one row per workflow per UTC day, for the last DAYS days (and today so far):
#   periods_due       hours, 4-hour blocks or days that have started
#   periods_started   periods with at least one run (scheduled or manual)
#   periods_complete  periods in which every script succeeded in some run
#   runs / scheduled_runs / noop_runs   attempts; a no-op found every script done
#   script_failures   failed steps, and which jobs they were
#   median_/max_minutes_after_slot      how late scheduled runs started
# A period with no run at all is one GitHub dropped every attempt for. Days
# before a workflow's first record are left out. Doesn't fail on gaps: the
# freshness check does that.

DAYS = 7
PER_DAY = {'Hourly': 24, 'Every 4 Hours': 6, 'Daily': 1}
KEY_TIME = re.compile(r'-(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})\.csv$')
COLUMNS = ['workflow', 'frequency', 'date', 'periods_due', 'periods_started', 'periods_complete', 'runs',
           'scheduled_runs', 'noop_runs', 'script_failures', 'failed_jobs', 'median_minutes_after_slot',
           'max_minutes_after_slot', 'first_record_date']

today = RUN_TIME.date()
since = today - timedelta(days=DAYS)
keys, first_dates = [], {}
for o in list_objects(PREFIX):
    m = KEY_TIME.search(o['Key'])
    if not m:
        continue
    date, wf = m.group(1)[:10], o['Key'][len(PREFIX):].split('/', 1)[0]
    first_dates[wf] = min(first_dates.get(wf, date), date)
    if date >= since.isoformat():
        keys.append(o['Key'])

with ThreadPoolExecutor(max_workers=16) as pool:
    frames = list(pool.map(lambda k: read_records([k]), keys))
records = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=['workflow'])
print(f'Read {len(keys)} run records since {since}')

m = manifest()
rows = []
for workflow in m['workflows']:
    frequency = workflow_frequency(m, workflow)
    first = first_dates.get(workflow)
    if first is None:
        continue
    wf = records[records['workflow'] == workflow].copy()
    if not wf.empty:
        wf['date'] = wf['period'].str[:10]
    for offset in range(DAYS, -1, -1):
        day = today - timedelta(days=offset)
        if day.isoformat() < first:
            continue
        hours_per_period = 24 // PER_DAY[frequency]
        if day < today:
            due = PER_DAY[frequency]
        else:  # periods that have started so far today, the current one included
            due = period_start(RUN_TIME, frequency).hour // hours_per_period + 1
        d = wf[wf['date'] == day.isoformat()] if not wf.empty else wf
        runs = d.groupby(['run_id', 'run_attempt']) if not d.empty else None
        complete = 0
        if not d.empty:
            for _, p in d.groupby('period'):
                if set(p['job']) <= set(p.loc[p['outcome'] == 'success', 'job']):
                    complete += 1
        sched = d[d['event'] == 'schedule'].drop_duplicates(['run_id', 'run_attempt']) if not d.empty else d
        late = pd.to_numeric(sched['minutes_after_slot'], errors='coerce').dropna() if not d.empty else pd.Series(dtype=float)
        failures = d[d['outcome'] == 'failure'] if not d.empty else d
        rows.append({
            'workflow': workflow, 'frequency': frequency, 'date': day.isoformat(), 'periods_due': due,
            'periods_started': d['period'].nunique() if not d.empty else 0,
            'periods_complete': complete,
            'runs': runs.ngroups if runs is not None else 0,
            'scheduled_runs': len(sched),
            'noop_runs': sum((g['outcome'] == 'already_done').all() for _, g in runs) if runs is not None else 0,
            'script_failures': len(failures),
            'failed_jobs': ' '.join(sorted(set(failures['job']))) if not failures.empty else '',
            'median_minutes_after_slot': round(late.median()) if not late.empty else None,
            'max_minutes_after_slot': round(late.max()) if not late.empty else None,
            'first_record_date': first,
        })

report = pd.DataFrame(rows, columns=COLUMNS)
for col in ['median_minutes_after_slot', 'max_minutes_after_slot']:
    report[col] = report[col].astype('Int64')
save_csv(report, 'data-pipeline-logs/run-summary/run-summary-' + RUN_LABEL + '.csv')

recent = report[report['date'] >= (today - timedelta(days=1)).isoformat()]
lines = ['| workflow | date | periods started / due | complete | runs (no-op) | failures | median / max min late |',
         '|---|---|---|---|---|---|---|']
for r in recent.itertuples():
    late = f'{r.median_minutes_after_slot} / {r.max_minutes_after_slot}' if pd.notna(r.median_minutes_after_slot) else ''
    fails = f'{r.script_failures} ({r.failed_jobs})' if r.script_failures else '0'
    lines.append(f'| {r.workflow} | {r.date} | {r.periods_started} / {r.periods_due} | {r.periods_complete} | '
                 f'{r.runs} ({r.noop_runs}) | {fails} | {late} |')
print('\n'.join(lines))
summary = os.environ.get('GITHUB_STEP_SUMMARY')
if summary:
    with open(summary, 'a') as f:
        f.write('## Workflow runs, yesterday and today so far (UTC)\n\n' + '\n'.join(lines) + '\n')
