"""Build the GitHub Pages site that summarizes every job in this repo.

Reads the workflows in .github/workflows/, the scripts in jobs/, the README's
Active and Inactive tables, and git history, then writes _site/index.html
(the template in site/index.html with the collected data inlined as JSON).

Run from anywhere:  python site/build.py [--out _site]
Needs PyYAML. Full git history (fetch-depth: 0) gives accurate "added" dates;
with a shallow clone they fall back to the oldest commit available.
"""

import argparse
import html
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
REPO = 'erikgregorywebb/scheduled-jobs'
BRANCH = 'main'
# This site's own deploy workflow; it isn't a job.
SKIP_WORKFLOWS = {'pages.yml', 'checks.yml'}
# Shared helpers in jobs/, not jobs themselves.
SKIP_SCRIPTS = {'common.py'}

SCRIPT_RE = re.compile(r'jobs/([A-Za-z0-9_.\-]+\.(?:py|r|R))')
ENV_RE = re.compile(
    r"""os\.environ(?:\.get)?\s*[\[(]\s*['"]([A-Z0-9_]+)['"]"""
    r"""|os\.getenv\(\s*['"]([A-Z0-9_]+)['"]"""
    r"""|Sys\.getenv\(\s*['"]([A-Z0-9_]+)['"]"""
)
# String literals that look like an S3 key prefix, e.g. 'apple-app-store/' or "mta/metro-north-lifts/".
PREFIX_RE = re.compile(r"""['"]([a-z0-9][a-z0-9_\-]*(?:/[a-z0-9_\-]+)*/)""")
# Credentials every job uses to write to S3; listing them on every row would add nothing.
IGNORED_ENV = {'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_DEFAULT_REGION'}
# Env var names that hold a secret; anything else (DRY_RUN, BACKFILL) is a run option.
SECRET_RE = re.compile(r'(KEY|TOKEN|SECRET|PASSWORD|CLIENT_ID|USERNAME|USER_AGENT)$')


def git(*args):
    try:
        return subprocess.run(['git', *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return ''


def md_inline(text):
    """Render the little markdown the README tables use: `code` and [links](url)."""
    out = html.escape(text.strip(), quote=False)
    out = re.sub(r'`([^`]+)`', r'<code>\1</code>', out)
    out = re.sub(
        r'\[([^\]]+)\]\(([^)\s]+)\)',
        lambda m: f'<a href="{html.escape(m.group(2))}" rel="noopener">{m.group(1)}</a>',
        out,
    )
    return out


def parse_readme():
    """Map script name -> {description, frequency, readme_section} from the README tables."""
    rows = {}
    section = None
    for line in (ROOT / 'README.md').read_text(encoding='utf-8').splitlines():
        heading = re.match(r'^##\s+(.*)', line)
        if heading:
            section = heading.group(1).strip()
            continue
        if not line.startswith('|') or set(line.replace('|', '').strip()) <= set('- '):
            continue
        cells = [c.strip() for c in line.strip().strip('|').split('|')]
        name = cells[0].strip('` ')
        if not SCRIPT_RE.match('jobs/' + name):
            continue  # header row
        if section == 'Active' and len(cells) >= 5:
            rows[name] = {'frequency': cells[2], 'description': md_inline(cells[3]), 'readme_section': section}
        elif len(cells) >= 3:
            rows[name] = {'frequency': None, 'description': md_inline(cells[2]), 'readme_section': section}
    return rows


def parse_manifest():
    """Map script name -> its S3 output stems, from jobs.yml (empty if there's no jobs.yml)."""
    path = ROOT / 'jobs.yml'
    if not path.exists():
        return {}
    doc = yaml.safe_load(path.read_text(encoding='utf-8')) or {}
    return {
        job['script']: [o['stem'] if isinstance(o, dict) else o for o in job.get('outputs', [])]
        for job in (doc.get('jobs') or {}).values()
    }


def _cron_values(field, limit):
    """The values a cron field of plain numbers, lists, ranges and steps matches, or None for anything else."""
    values = set()
    for part in field.split(','):
        rng, _, step = part.partition('/')
        if rng == '*':
            lo, hi = 0, limit - 1
        elif re.fullmatch(r'\d+-\d+', rng):
            lo, hi = map(int, rng.split('-'))
        elif rng.isdigit():
            lo = int(rng)
            hi = limit - 1 if step else lo
        else:
            return None
        values.update(range(lo, hi + 1, int(step) if step else 1))
    return sorted(values)


def describe_cron(expr):
    """Plain-English description of the cron shapes this repo uses; falls back to the raw expression."""
    parts = expr.split()
    if len(parts) != 5 or tuple(parts[2:]) != ('*', '*', '*'):
        return f'cron {expr}'
    minutes, hours = _cron_values(parts[0], 60), _cron_values(parts[1], 24)
    if not minutes or not hours:
        return f'cron {expr}'
    at_minutes = ', '.join(f':{m:02d}' for m in minutes)
    if parts[1] == '*':
        return f'Tried hourly at {at_minutes}'
    step = re.fullmatch(r'\*/(\d+)', parts[1])
    if step:
        return f'Tried every {step.group(1)} hours at {at_minutes}'
    return 'Tried at ' + ', '.join(f'{h:02d}:{m:02d}' for h in hours for m in minutes) + ' UTC'


def runs_per_day(expr):
    """How many times a day the cron fires."""
    parts = expr.split()
    if len(parts) != 5 or tuple(parts[2:]) != ('*', '*', '*'):
        return None
    minutes, hours = _cron_values(parts[0], 60), _cron_values(parts[1], 24)
    return len(minutes) * len(hours) if minutes and hours else None


# Each workflow is tried several times per period, but its scripts run once per
# period (tools/run_log.py), so a job's runs per day come from its frequency.
RUNS_PER_DAY = {'Hourly': 24, 'Every 4 Hours': 6, 'Daily': 1}


def cron_comment(path, expr):
    """The trailing comment on the cron line, which in this repo explains the schedule."""
    for line in path.read_text(encoding='utf-8').splitlines():
        if expr in line and '#' in line:
            return line.split('#', 1)[1].strip()
    return None


def parse_workflows():
    workflows = []
    for path in sorted((ROOT / '.github' / 'workflows').glob('*.y*ml')):
        if path.name in SKIP_WORKFLOWS:
            continue
        doc = yaml.safe_load(path.read_text(encoding='utf-8')) or {}
        triggers = doc.get('on', doc.get(True)) or {}  # PyYAML reads the key `on` as True
        if isinstance(triggers, str):
            triggers = {triggers: None}
        elif isinstance(triggers, list):
            triggers = {t: None for t in triggers}
        crons = [s['cron'] for s in (triggers.get('schedule') or []) if isinstance(s, dict) and 'cron' in s]
        jobs = doc.get('jobs') or {}
        scripts, timeouts, runtimes = [], [], set()
        for job in jobs.values():
            if isinstance(job.get('timeout-minutes'), int):
                timeouts.append(job['timeout-minutes'])
            for step in job.get('steps') or []:
                uses = step.get('uses', '')
                if 'setup-python' in uses:
                    version = (step.get('with') or {}).get('python-version')
                    runtimes.add(f'Python {version}' if version else 'Python')
                elif 'setup-r' in uses:
                    runtimes.add('R')
                for name in SCRIPT_RE.findall(str(step.get('run', ''))):
                    if not any(s['script'] == name for s in scripts):
                        scripts.append({
                            'script': name,
                            'step_name': step.get('name'),
                            'step_timeout': step.get('timeout-minutes'),
                        })
        workflows.append({
            'file': path.name,
            'name': doc.get('name') or path.stem,
            'crons': [
                {'expr': c, 'text': describe_cron(c), 'per_day': runs_per_day(c), 'comment': cron_comment(path, c)}
                for c in crons
            ],
            'manual': 'workflow_dispatch' in triggers,
            'scheduled': bool(crons),
            'timeout': max(timeouts) if timeouts else None,
            'runtimes': sorted(runtimes),
            'scripts': scripts,
        })
    return workflows


def script_meta(path):
    text = path.read_text(encoding='utf-8', errors='replace')
    lines = text.splitlines()
    env = []
    for match in ENV_RE.finditer(text):
        name = next(g for g in match.groups() if g)
        if name not in IGNORED_ENV and name not in env:
            env.append(name)
    prefixes = []
    for line in lines:
        if line.lstrip().startswith('#') or not re.search(r'file_?path|PREFIX|filename|object|Key=|writeToS3', line, re.I):
            continue
        for p in PREFIX_RE.findall(line):
            if not p.startswith(('http', 'www')) and p not in prefixes:
                prefixes.append(p)
    log = git('log', '--follow', '--format=%cI%x1f%s', '--', str(path.relative_to(ROOT))).splitlines()
    commits = [l.split('\x1f', 1) for l in log if '\x1f' in l]
    return {
        'lines': sum(1 for l in lines if l.strip() and not l.strip().startswith('#')),
        'secrets': [e for e in env if SECRET_RE.search(e)],
        'options': [e for e in env if not SECRET_RE.search(e)],
        's3_prefixes': prefixes,
        'commits': len(commits),
        'last_changed': commits[0][0] if commits else None,
        'last_change_subject': commits[0][1] if commits else None,
        'added': commits[-1][0] if commits else None,
    }


def build():
    readme = parse_readme()
    manifest_outputs = parse_manifest()
    workflows = parse_workflows()
    scheduled_by = {}
    for wf in workflows:
        for s in wf['scripts']:
            scheduled_by.setdefault(s['script'], []).append((wf, s))

    jobs = []
    for path in sorted((ROOT / 'jobs').iterdir()):
        if not SCRIPT_RE.match('jobs/' + path.name) or path.name in SKIP_SCRIPTS:
            continue
        entries = scheduled_by.get(path.name, [])
        live = [(wf, s) for wf, s in entries if wf['scheduled']]
        crons = [c for wf, _ in live for c in wf['crons']]
        info = readme.get(path.name, {})
        jobs.append({
            'script': path.name,
            'language': 'Python' if path.suffix == '.py' else 'R',
            'status': 'active' if live else 'inactive',
            'workflows': [wf['name'] for wf, _ in live],
            'test_workflows': [wf['name'] for wf, s in entries if not wf['scheduled']],
            'schedule': ', '.join(filter(None, [info.get('frequency')] + [c['text'][0].lower() + c['text'][1:] for c in crons]))
                        if crons else None,
            'runs_per_day': (RUNS_PER_DAY.get(info.get('frequency')) if live else None)
                            or sum(c['per_day'] or 0 for c in crons) or None,
            'frequency': info.get('frequency'),
            'step_timeout': next((s['step_timeout'] for _, s in live if s['step_timeout']), None),
            'step_name': next((s['step_name'] for _, s in live if s['step_name']), None),
            'workflow_files': [wf['file'] for wf, _ in live],
            'description': info.get('description'),
            'in_readme': path.name in readme,
            **script_meta(path),
        })
        # jobs.yml lists the exact S3 key stems each job writes; prefer them to the regex guess
        stems = manifest_outputs.get(path.name)
        if stems:
            jobs[-1]['s3_prefixes'] = sorted({s.rsplit('/', 1)[0] + '/' for s in stems})

    head = git('rev-parse', 'HEAD')
    return {
        'repo': REPO,
        'branch': BRANCH,
        'generated_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'commit': head,
        'commit_short': head[:7],
        'workflows': workflows,
        'jobs': jobs,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--out', default=str(ROOT / '_site'), help='output directory (default: _site)')
    args = parser.parse_args()

    data = build()
    template = (Path(__file__).parent / 'index.html').read_text(encoding='utf-8')
    # Inline as JSON; escape "</" so a description can never close the <script> tag.
    payload = json.dumps(data, ensure_ascii=False).replace('</', '<\\/')
    page = template.replace('/*__DATA__*/null', payload)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / 'index.html').write_text(page, encoding='utf-8')
    (out / 'jobs.json').write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')
    (out / '.nojekyll').write_text('', encoding='utf-8')
    active = sum(j['status'] == 'active' for j in data['jobs'])
    print(f"Wrote {out / 'index.html'}: {len(data['jobs'])} scripts ({active} active), {len(data['workflows'])} workflows")


if __name__ == '__main__':
    main()
