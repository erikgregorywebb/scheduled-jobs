# scheduled-jobs

Python scripts that collect public data on a schedule (GitHub Actions) and save
timestamped snapshots to the private S3 bucket `egw-data-dumps`. Most sources
only show what's true right now, so these files are the history; never lose or
silently corrupt them.

## Layout

- `jobs/` — one script per job, plus `common.py` (shared helpers) and
  `freshness-check.py` (the daily monitor, itself a job).
- `jobs.yml` — the one list of jobs: script, workflow, frequency, step timeout,
  secrets, S3 output stems, README description. **Source of truth.**
- `tools/manifest.py` — generates `.github/workflows/job_*.yml`,
  `job_test_python.yml` and the README tables from `jobs.yml`. Run
  `python tools/manifest.py` after editing `jobs.yml`; `--check` validates (CI
  runs it in `checks.yml`). Never hand-edit the generated files.
- `site/` — the GitHub Pages summary (https://erikgregorywebb.github.io/scheduled-jobs/),
  built by `site/build.py` from the workflows, README tables, `jobs.yml` and git
  history, and deployed by `pages.yml` on pushes to main. It reads the README's
  Active table by column position, which `manifest.py` keeps stable.
- `archive/` — retired scripts, including the R originals of jobs ported to
  Python in 2026-11. Not scheduled; R is no longer used.

## Writing a job

Use `jobs/common.py`; don't re-implement its pieces:

- `get` / `get_json` / `post` / `request`: timeout plus retries on connection
  errors, timeouts, 5xx and 429 (after 10, 30, 60s by default; `delays=` to
  change). Any other 4xx raises straight away. `session=` takes a
  `requests.Session`.
- `save_csv(df, key)` / `save_bytes(body, key)`: write to S3, or under `out/`
  when `DRY_RUN=1`. CSVs have a header and no index.
- `RUN_DATETIME` (a `datetime` column value) and `RUN_LABEL` (the timestamp in
  keys): one UTC timestamp per run. Never use naive `datetime.now()`. All
  timestamps are UTC unless the column name says otherwise (e.g. `_et`).
- `s3()`, `list_objects`, `latest_object`, `object_exists`, `same_as_latest`:
  reading the bucket, e.g. to skip saving a file that hasn't changed (Zillow,
  FAA registry).

Conventions (see `.github/pull_request_template.md` for the full checklist):

- S3 key: `<prefix>/<name>-<RUN_LABEL>.csv`. Its stem (the key up to the
  timestamp) goes in the job's `outputs` in `jobs.yml`, so the freshness check
  watches it. Don't change an existing job's keys or columns without saying so
  plainly: analyses read them across years of files.
- Fail loudly on bad data (error page, empty or truncated response) rather
  than saving it; a quiet run should still write a file (header only) so it can
  be told apart from a missed one.
- Secrets come from environment variables listed under the job's `secrets` in
  `jobs.yml`. `manifest.py --check` fails if a script reads an env var that
  isn't declared.
- New packages: pin in `requirements.txt`, check they install on Python 3.10.
  `boto3==1.24.0` caps urllib3 below 1.27; bumping boto3 lifts that.
- Be polite to sources: keep existing sleeps and concurrency limits, set a
  descriptive User-Agent where a site needs one, and check robots.txt or terms
  for new sources.

## Testing

- Local: `DRY_RUN=1 python jobs/<script>.py` writes under `out/` (gitignored)
  and never uploads. Compare its columns with the newest real file for the same
  stem before calling a change done. Python 3.10 matches the runners.
- Runner: the `job_test_python` workflow (manual) runs named scripts on a
  branch with every secret. With `dry_run` ticked, files go to an Actions
  artifact instead of S3. Without it, it writes real snapshots, the usual way
  to test scripts that need secrets.
- Scripts needing secrets (PRAW_*, NEWSAPI_KEY, LAST_FM_API_KEY, TMDB_API_KEY,
  NYC_OPEN_DATA_APP_TOKEN) can only be fully tested on a runner.

## Schedules

| Workflow | When (UTC) | What |
| --- | --- | --- |
| job_1 | daily 08:13 | most daily jobs (AAA gas prices takes ~17 min at its crawl delay) |
| job_2 | every 4h at :17 | Reddit comments |
| job_3 | daily 08:29 | the jobs ported from R |
| job_4 | daily 08:43 | Pinal County parcels (slow) |
| job_5 | hourly at :07 | weather, Citi Bike, FAA status, Metro-North, Polymarket |
| job_6 | daily 06:23 | previous day's weather |
| job_7 | daily 14:37 | Ski Utah snow reports (after resorts post) |
| job_8 | daily 11:47 | freshness check |

Crons sit off the top of the hour, when GitHub's queue is busiest. Scheduled
runs can start late or be skipped, so the freshness limits (Hourly 4h, Every 4
Hours 9h, Daily 26h) leave slack.

## Bucket notes

- `data-pipeline-logs/s3-changes/`: daily list of objects added, modified or
  removed (s3-snapshots). `data-pipeline-logs/s3-snapshot-state.csv` is the
  current full listing. The old daily full listings
  (`data-pipeline-logs/s3-snapshot-<ts>.csv`) stop on 2026-11-27.
- `data-pipeline-logs/freshness/`: the freshness check's daily report.
- `zillow/`: deduplicated on 2026-11-27, keeping the first copy of each of the
  44 distinct files. `data-pipeline-logs/zillow-dedupe/` maps every removed key
  to the kept copy. Bucket versioning is off, so deletes are permanent: list and
  verify before deleting anything.
