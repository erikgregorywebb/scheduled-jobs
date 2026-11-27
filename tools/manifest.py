"""Generate the scheduled workflows and the README job tables from jobs.yml.

    python tools/manifest.py           rewrite .github/workflows/job_*.yml and README.md
    python tools/manifest.py --check   exit 1 if anything is out of date or invalid (CI)

jobs.yml is the one place to add, move, reschedule or retire a job. The
generated files say so at the top; edit jobs.yml and re-run this instead of
editing them by hand.
"""
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / 'jobs.yml'
WORKFLOWS = ROOT / '.github' / 'workflows'
README = ROOT / 'README.md'
README_BEGIN = '<!-- BEGIN GENERATED FROM jobs.yml: edit jobs.yml and run `python tools/manifest.py` -->'
README_END = '<!-- END GENERATED FROM jobs.yml -->'

ALWAYS_SECRETS = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
# environment variables scripts may read that aren't secrets
NON_SECRET_ENV = {'DRY_RUN', 'DRY_RUN_DIR', 'BACKFILL', 'CRAWL_DELAY', 'PARSE_FILE', 'GITHUB_STEP_SUMMARY'}
FREQUENCIES = {'Hourly': 4, 'Every 4 Hours': 9, 'Daily': 26}  # default freshness max age, hours
HEADER = '# Generated from jobs.yml by tools/manifest.py. Edit jobs.yml and re-run it; don\'t edit this file.\n'
PYTHON_SETUP = """\
      - name: checkout repo content
        uses: actions/checkout@v5

      - name: setup python
        uses: actions/setup-python@v6
        with:
          python-version: '3.10'
          cache: 'pip' # reuse downloaded packages between runs

      - name: install python packages
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
"""


def load():
    with open(MANIFEST) as f:
        return yaml.safe_load(f)


def comment(note):
    return f' # {note}' if note else ''


def env_block(secrets, indent):
    pad = ' ' * indent
    lines = [f'{pad}env:']
    for name in ALWAYS_SECRETS + list(secrets):
        lines.append(f'{pad}  {name}: ${{{{ secrets.{name} }}}}')
    return '\n'.join(lines) + '\n'


def render_workflow(name, wf, jobs):
    out = [HEADER, f'# {wf["description"]}\n' if wf.get('description') else '', f'\nname: {name}\n\non:\n  workflow_dispatch:\n  schedule:\n',
           f"    - cron: '{wf['cron']}'{comment(wf.get('cron_note'))}\n\n",
           'jobs:\n  build:\n    runs-on: ubuntu-latest\n',
           f'    timeout-minutes: {wf["timeout"]}{comment(wf.get("timeout_note"))}\n    steps:\n\n',
           PYTHON_SETUP, '\n',
           '      # Each script runs in its own step, so one failing or hung script doesn\'t skip\n'
           '      # the rest; the run is still marked failed if any step fails. Each step gets\n'
           '      # only the secrets its script needs.\n']
    for job_name, job in jobs:
        out.append(f'\n      - name: run {job_name}\n')
        out.append('        if: ${{ !cancelled() }}\n')
        out.append(f'        timeout-minutes: {job["timeout"]}{comment(job.get("timeout_note"))}\n')
        out.append(env_block(job.get('secrets', []), 8))
        out.append(f'        run: python jobs/{job["script"]}\n')
    return ''.join(out)


def render_test_workflow(all_secrets):
    return HEADER + """
name: job_test_python

on:
  workflow_dispatch: # manual runs only, from the Actions tab
    inputs:
      scripts:
        description: 'Space-separated scripts in jobs/ to run, e.g. "last-fm-history.py ny-lottery-winning-numbers.py"'
        required: true
        default: 'wikipedia-daily-metrics.py'
      dry_run:
        description: 'Dry run: write files to an Actions artifact instead of S3'
        type: boolean
        default: false

jobs:
  build:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:

""" + PYTHON_SETUP + """
      - name: run scripts
""" + env_block(all_secrets, 8).rstrip('\n') + """
          SCRIPTS: ${{ inputs.scripts }} # passed via env, not inlined into the shell
          DRY_RUN: ${{ inputs.dry_run && '1' || '' }}
        run: |
          # run every script even if one fails, then fail the step if any did
          failed=""
          for script in $SCRIPTS; do
            case "$script" in
              */*|..*) echo "Expected a file name in jobs/, got: $script"; exit 1 ;;
            esac
            echo "::group::$script"
            python "jobs/$script" || { echo "::error::$script failed"; failed="$failed $script"; }
            echo "::endgroup::"
          done
          if [ -n "$failed" ]; then echo "Failed:$failed"; exit 1; fi

      - name: upload dry-run files
        if: ${{ !cancelled() && inputs.dry_run }}
        uses: actions/upload-artifact@v4
        with:
          name: dry-run-output
          path: out/
          if-no-files-found: warn
"""


def md(text):
    return ' '.join(str(text).split()).replace('|', '\\|')


def render_readme_tables(m):
    lines = [README_BEGIN, '', '## Active', '',
             'Scheduled by the GitHub Actions workflow in the Workflow column (see `jobs.yml`).', '',
             '| Name | Type | Frequency | Description | Workflow |', '| --- | --- | --- | --- | --- |']
    # site/build.py reads these columns by position: keep Frequency 3rd and Description 4th
    for name, job in m['jobs'].items():
        lines.append(f'| `{job["script"]}` | Python | {job["frequency"]} | {md(job["description"])} | `{job["workflow"]}` |')
    lines += ['', '## Inactive', '', 'Not scheduled; kept in `archive/` for reference.', '',
              '| Name | Type | Description |', '| --- | --- | --- |']
    for item in m.get('inactive', []):
        lines.append(f'| `{item["script"]}` | {item["type"]} | {md(item["description"])} |')
    lines += ['', README_END]
    return '\n'.join(lines)


ENV_READ = re.compile(r"""os\.(?:environ\[|environ\.get\(|getenv\()\s*['"]([A-Z0-9_]+)['"]""")


def validate(m):
    errors = []
    workflows = m['workflows']
    for name, job in m['jobs'].items():
        where = f'jobs.{name}'
        for field in ['script', 'workflow', 'frequency', 'timeout', 'description', 'outputs']:
            if field not in job:
                errors.append(f'{where}: missing {field}')
        script = ROOT / 'jobs' / job.get('script', '')
        if not script.is_file():
            errors.append(f'{where}: jobs/{job.get("script")} does not exist')
            continue
        if job.get('workflow') not in workflows:
            errors.append(f'{where}: unknown workflow {job.get("workflow")}')
        if job.get('frequency') not in FREQUENCIES:
            errors.append(f'{where}: frequency must be one of {list(FREQUENCIES)}')
        declared = set(ALWAYS_SECRETS) | set(job.get('secrets', [])) | NON_SECRET_ENV
        for var in sorted(set(ENV_READ.findall(script.read_text())) - declared):
            errors.append(f'{where}: jobs/{job["script"]} reads {var}; add it to secrets')
        for out in job.get('outputs', []):
            stem = out['stem'] if isinstance(out, dict) else out
            if not isinstance(stem, str) or not stem.endswith(('-', '/')):
                errors.append(f'{where}: output stem {stem!r} should be the key up to the timestamp, ending in - or /')
    listed = {j['script'] for j in m['jobs'].values()}
    for path in sorted((ROOT / 'jobs').glob('*.py')):
        if path.name != 'common.py' and path.name not in listed:
            errors.append(f'jobs/{path.name} is not in jobs.yml (move it to archive/ if it is retired)')
    for item in m.get('inactive', []):
        if not (ROOT / item['script']).is_file():
            errors.append(f'inactive: {item["script"]} does not exist')
    for name in workflows:
        if not any(j['workflow'] == name for j in m['jobs'].values()):
            errors.append(f'workflows.{name} has no jobs')
    return errors


def outputs():
    """Every generated file: {path: contents}."""
    m = load()
    files = {}
    for name, wf in m['workflows'].items():
        jobs = [(n, j) for n, j in m['jobs'].items() if j['workflow'] == name]
        files[WORKFLOWS / f'{name}.yml'] = render_workflow(name, wf, jobs)
    all_secrets = sorted({s for j in m['jobs'].values() for s in j.get('secrets', [])})
    files[WORKFLOWS / 'job_test_python.yml'] = render_test_workflow(all_secrets)
    readme = README.read_text()
    start, end = readme.find(README_BEGIN), readme.find(README_END)
    if start == -1 or end == -1:
        raise SystemExit(f'README.md needs the markers:\n{README_BEGIN}\n{README_END}')
    files[README] = readme[:start] + render_readme_tables(m) + readme[end + len(README_END):]
    return m, files


def main():
    check = '--check' in sys.argv
    m, files = outputs()
    errors = validate(m)
    generated_names = {p.name for p in files}
    for path in WORKFLOWS.glob('job_*.yml'):
        if path.name not in generated_names:
            errors.append(f'{path.relative_to(ROOT)} is not generated from jobs.yml; remove it or add the workflow')
    stale = [p for p, text in files.items() if not p.exists() or p.read_text() != text]
    if check:
        for p in stale:
            errors.append(f'{p.relative_to(ROOT)} is out of date; run python tools/manifest.py')
    else:
        for p in stale:
            p.write_text(files[p])
            print(f'wrote {p.relative_to(ROOT)}')
    for e in errors:
        print(f'ERROR: {e}')
    if errors:
        sys.exit(1)
    if check:
        print(f'jobs.yml OK: {len(m["jobs"])} jobs in {len(m["workflows"])} workflows; generated files up to date')


if __name__ == '__main__':
    main()
