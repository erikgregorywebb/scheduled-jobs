<!--
PR guide for scheduled-jobs. Delete these comments, and any section that
doesn't apply, before opening the PR.

Title: say what changes and where it runs, e.g.
  "Add hourly Metro-North service alerts job to job_5"
  "Revive the LDS meetinghouses scraper in Python"
For a fix, name the problem it fixes, not only the change.
-->

Adds `jobs/<script>` as its own step in `job_<N>`, which runs <hourly / daily / every 4 hours>.

## What it does
<!--
- The source: the URL or endpoint, and what it holds.
- Why snapshots are worth keeping: is the source overwritten, or only showing
  what's true right now? Then these files are the history.
- Access: needs an API key (name the secret) or none; robots.txt / terms checked (date).
- Anything unusual about the source: undocumented fields, a required
  User-Agent, rate limits, paging, how big a response is.
-->

## What it writes
<!--
- S3 key pattern, e.g. `<prefix>/<name>-YYYY-mm-dd-HH-MM-SS.csv` in egw-data-dumps.
- The grain: "one row per ___".
- The main columns, and the time zone of every timestamp.
- Whether columns are fixed, and whether a file is written when there's nothing
  to report (so a quiet run can be told apart from a missed one).
- Rough size per run, if it's more than a few MB.
-->

## Testing
<!--
Give real numbers from real runs, not "it works".
- Local dry run against the live source: rows, and any counts that show
  the output is right.
- A local `DRY_RUN=1` run, and a `job_test_python` run on this branch, with
  a link, and the S3 key it wrote.
- Any edge case you ran: an empty feed, a failed request, the pinned
  package versions in requirements.txt.
Say plainly what wasn't tested.
-->

## Other changes
<!-- requirements.txt pins (and that they install on Python 3.10), jobs.yml entries, anything in common.py. -->

## Notes for using the data
<!-- Gotchas for whoever analyses it later: duplicates across snapshots, NA values, IDs that don't join, gaps in coverage. -->

## Checklist
<!-- Tick what's done; leave a note next to anything that doesn't apply. -->
- [ ] Script is in `jobs/` and its name matches the source and what it collects
- [ ] Uses `jobs/common.py`: `get`/`get_json` for requests (timeouts, retries on 5xx/429/connection errors, fail fast on other 4xx), `save_csv`/`save_bytes` for S3, `RUN_DATETIME`/`RUN_LABEL` (UTC) for timestamps
- [ ] Credentials come from environment variables listed under the job's `secrets` in `jobs.yml`, never from the code
- [ ] Fails loudly on bad data (error page, truncated or empty response) rather than saving it
- [ ] New packages are pinned in `requirements.txt` and install with the pinned versions on Python 3.10
- [ ] An entry in `jobs.yml` (workflow, frequency, timeout, secrets, output stems, description), then `python tools/manifest.py` to regenerate the workflows and README; retired scripts go to `archive/` and `inactive`
- [ ] Ran on a real runner with `job_test_python`, or says why not
