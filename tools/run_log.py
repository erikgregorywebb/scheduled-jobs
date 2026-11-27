"""Skip work already done this period, and record every attempt of a workflow.

GitHub's scheduler delays and drops scheduled runs, so each workflow in
jobs.yml is scheduled several times per period (an hour, a 4-hour block or a
UTC day, from its jobs' frequency). The first attempt to start in a period runs
the scripts; later attempts run only the scripts that haven't succeeded yet,
which is usually none. Every run, scheduled or manual, writes one row per
script to

    data-pipeline-logs/runs/<workflow>/runs-<workflow>-<YYYY-mm-dd-HH-MM-SS>.csv

and jobs/run-summary.py rolls those up daily. The generated workflows call:

    python tools/run_log.py gate     first step: which scripts already succeeded
                                     this period (writes `done` etc. to $GITHUB_OUTPUT)
    python tools/run_log.py record   last step, always: this run's rows (reads the
                                     steps context from $STEPS)

Manual runs (workflow_dispatch) always run every script, and count towards the
period like any other run. If the bucket can't be read, the gate lets every
script run: a duplicate snapshot is better than a missing one.
"""
import io
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'jobs'))
from common import BUCKET, list_objects, s3, save_csv  # noqa: E402

PREFIX = 'data-pipeline-logs/runs/'
COLUMNS = ['workflow', 'period', 'run_id', 'run_attempt', 'event', 'cron', 'slot_utc', 'started_at_utc',
           'finished_at_utc', 'minutes_after_slot', 'job', 'script', 'outcome', 'commit']
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
LABEL_FORMAT = '%Y-%m-%d-%H-%M-%S'


def manifest():
    with open(ROOT / 'jobs.yml') as f:
        return yaml.safe_load(f)


def workflow_jobs(m, workflow):
    """[(job name, job)] in the order the workflow runs them."""
    return [(n, j) for n, j in m['jobs'].items() if j['workflow'] == workflow]


def workflow_frequency(m, workflow):
    freqs = {j['frequency'] for _, j in workflow_jobs(m, workflow)}
    if len(freqs) != 1:
        raise SystemExit(f'{workflow}: jobs have frequencies {sorted(freqs)}; expected exactly one')
    return freqs.pop()


def period_start(t, frequency):
    """Start of the period `t` falls in: its hour, 4-hour block (00, 04, ... UTC) or UTC day."""
    t = t.replace(minute=0, second=0, microsecond=0)
    if frequency == 'Hourly':
        return t
    if frequency == 'Every 4 Hours':
        return t.replace(hour=t.hour - t.hour % 4)
    if frequency == 'Daily':
        return t.replace(hour=0)
    raise ValueError(f'unknown frequency {frequency!r}')


def period_label(t, frequency):
    return period_start(t, frequency).strftime('%Y-%m-%d %H:%M')


def _cron_field(field, value, low):
    for part in field.split(','):
        rng, _, step = part.partition('/')
        step = int(step) if step else 1
        if rng == '*':
            lo, hi = low, 10 ** 6
        elif '-' in rng:
            lo, hi = map(int, rng.split('-'))
        else:
            lo = int(rng)
            hi = 10 ** 6 if '/' in part else lo
        if lo <= value <= hi and (value - lo) % step == 0:
            return True
    return False


def cron_matches(expr, t):
    minute, hour, dom, month, dow = expr.split()
    return (_cron_field(minute, t.minute, 0) and _cron_field(hour, t.hour, 0) and _cron_field(dom, t.day, 1)
            and _cron_field(month, t.month, 1) and _cron_field(dow, (t.weekday() + 1) % 7, 0))


def last_slot(expr, t, max_days=2):
    """The latest time at or before `t` that cron `expr` fires, or None. GitHub
    doesn't say which slot a late run was for, so this is the nearest one: a
    run delayed past the next slot is counted against that one."""
    t = t.replace(second=0, microsecond=0)
    for i in range(max_days * 1440):
        candidate = t - timedelta(minutes=i)
        if cron_matches(expr, candidate):
            return candidate
    return None


def read_records(keys):
    frames = []
    for key in keys:
        body = s3().get_object(Bucket=BUCKET, Key=key)['Body'].read()
        frames.append(pd.read_csv(io.BytesIO(body), dtype=str))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS)


def write_output(**values):
    path = os.environ.get('GITHUB_OUTPUT')
    lines = ''.join(f'{k}={v}\n' for k, v in values.items())
    if path:
        with open(path, 'a') as f:
            f.write(lines)
    else:
        print(lines, end='')


def gate():
    workflow = os.environ['GITHUB_WORKFLOW']
    event = os.environ.get('GITHUB_EVENT_NAME', '')
    m = manifest()
    frequency = workflow_frequency(m, workflow)
    now = datetime.now(timezone.utc)
    period = period_label(now, frequency)
    done = []
    if event == 'schedule':
        # a run's key starts with its start time, which is always inside its period
        start = period_start(now, frequency)
        stamp = start.strftime('%Y-%m-%d-%H') if frequency == 'Hourly' else start.strftime('%Y-%m-%d')
        keys = []
        try:
            keys = [o['Key'] for o in list_objects(f'{PREFIX}{workflow}/runs-{workflow}-{stamp}')]
            records = read_records(keys)
            ok = records[(records['period'] == period) & (records['outcome'] == 'success')]
            done = sorted(set(ok['job']))
        except Exception as e:  # fail open: run everything rather than miss a period
            print(f'::warning::Could not read earlier runs, running every script: {type(e).__name__}: {e}')
            done = []
        names = [n for n, _ in workflow_jobs(m, workflow)]
        todo = [n for n in names if n not in done]
        print(f'{workflow} period {period}: {len(keys)} earlier run(s) this period; '
              f'{len(done)} of {len(names)} scripts already succeeded; running {len(todo)}: {", ".join(todo) or "none"}')
    else:
        print(f'{workflow} {event} run: running every script (period {period})')
    write_output(done=',' + ','.join(done) + ',', period=period, started_at=now.strftime(TIME_FORMAT),
                 label=now.strftime(LABEL_FORMAT))


def record():
    workflow = os.environ['GITHUB_WORKFLOW']
    event = os.environ.get('GITHUB_EVENT_NAME', '')
    steps = json.loads(os.environ.get('STEPS') or '{}')
    m = manifest()
    frequency = workflow_frequency(m, workflow)
    now = datetime.now(timezone.utc)
    gate_out = (steps.get('gate') or {}).get('outputs') or {}
    started = (datetime.strptime(gate_out['started_at'], TIME_FORMAT).replace(tzinfo=timezone.utc)
               if gate_out.get('started_at') else now)
    period = gate_out.get('period') or period_label(started, frequency)
    label = gate_out.get('label') or started.strftime(LABEL_FORMAT)
    done = set(filter(None, gate_out.get('done', '').split(',')))

    cron = ''
    event_path = os.environ.get('GITHUB_EVENT_PATH')
    if event == 'schedule' and event_path and os.path.exists(event_path):
        with open(event_path) as f:
            cron = json.load(f).get('schedule', '') or ''
    slot = last_slot(cron, started) if cron else None

    rows = []
    for name, job in workflow_jobs(m, workflow):
        step = steps.get(name)
        if name in done:
            outcome = 'already_done'
        elif step is None:
            outcome = 'not_run'
        else:
            outcome = step.get('outcome') or 'unknown'
        rows.append({
            'workflow': workflow, 'period': period,
            'run_id': os.environ.get('GITHUB_RUN_ID', ''), 'run_attempt': os.environ.get('GITHUB_RUN_ATTEMPT', ''),
            'event': event, 'cron': cron,
            'slot_utc': slot.strftime(TIME_FORMAT) if slot else '',
            'started_at_utc': started.strftime(TIME_FORMAT), 'finished_at_utc': now.strftime(TIME_FORMAT),
            'minutes_after_slot': round((started - slot).total_seconds() / 60) if slot else '',
            'job': name, 'script': job['script'], 'outcome': outcome,
            'commit': os.environ.get('GITHUB_SHA', '')[:7],
        })
    df = pd.DataFrame(rows, columns=COLUMNS)
    save_csv(df, f'{PREFIX}{workflow}/runs-{workflow}-{label}.csv')

    counts = df['outcome'].value_counts()
    print(f'{workflow} period {period}: ' + ', '.join(f'{n} {o}' for o, n in counts.items()))
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a') as f:
            delay = f', {rows[0]["minutes_after_slot"]} min after the {slot:%H:%M} UTC slot' if slot else ''
            f.write(f'**{workflow}**, period {period} ({event}{delay}): '
                    + ', '.join(f'{n} {o}' for o, n in counts.items()) + '\n')


if __name__ == '__main__':
    commands = {'gate': gate, 'record': record}
    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        raise SystemExit(f'usage: python tools/run_log.py {{{"|".join(commands)}}}')
    commands[sys.argv[1]]()
