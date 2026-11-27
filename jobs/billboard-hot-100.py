from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import os
import pandas as pd

from common import DRY_RUN, RUN_DATETIME, RUN_TIME, get, object_exists, save_bytes

# Saves the Billboard Hot 100 (https://www.billboard.com/charts/hot-100/) to S3: one row per song
# with rank, title, artist, last week's rank, peak rank, weeks at No. 1 (for songs that peaked
# there), weeks on chart and a new / re-entry flag.
# Billboard publishes the chart on Tuesdays, dated the Saturday of the following week, and the page
# only ever shows the current chart. This runs daily and saves a chart only the first time it sees
# that chart date, so each saved file is one weekly chart.
# robots.txt (checked 2026-11-27) allows /charts/ for User-agent: *.
# There's no API: the chart is parsed from the page's HTML. If Billboard changes the layout,
# the checks below fail the run (and print what they found) rather than saving a bad chart.
#
# PARSE_FILE=page.html python jobs/billboard-hot-100.py parses a saved page instead of fetching
# (and implies DRY_RUN); DRY_RUN=1 fetches and parses without touching S3.

URL = 'https://www.billboard.com/charts/hot-100/'
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; scheduled-jobs data collector)'}
PREFIX = 'billboard-hot-100/'
COLUMNS = ['chart_date', 'rank', 'title', 'artist', 'last_week', 'peak_position',
           'weeks_at_no_1', 'weeks_on_chart', 'status']
MONTHS = 'January|February|March|April|May|June|July|August|September|October|November|December'


def fetch(url):
    # retries 5xx, 429 and connection errors (after 15, 30, 45s); any other 4xx fails right away
    return get(url, headers=HEADERS, timeout=60, delays=(15, 30, 45))


def clean(text):
    return re.sub(r'\s+', ' ', text or '').strip()


def chart_date_of(soup, html):
    # the date picker carries the chart date as data-date="YYYY-MM-DD"; fall back to "Week of ..."
    picker = soup.find(id='chart-date-picker')
    if picker is not None and picker.get('data-date'):
        return datetime.strptime(picker['data-date'], '%Y-%m-%d').date(), 'chart-date-picker'
    match = re.search(r'Week of\s+(' + MONTHS + r')\s+(\d{1,2}),\s+(\d{4})', html)
    if match:
        return datetime.strptime(' '.join(match.groups()), '%B %d %Y').date(), '"Week of" text'
    return None, None


# stat labels on each row, each followed by its value in the next span; the row shows the block
# twice (desktop and mobile layouts, in different orders), and both copies must agree
LABELS = {'LW': 'last_week', 'PEAK': 'peak_position', 'WEEKS AT NO. 1': 'weeks_at_no_1',
          'WEEKS ON CHART': 'weeks_on_chart'}


def parse_row(row):
    # one <ul class="o-chart-results-list-row"> per song: rank, title, artist, then the stats
    title_el = row.find('h3', id='title-of-a-story') or row.find('h3')
    if title_el is None:
        return None
    artist_el = title_el.find_next_sibling('span')
    labels = [clean(s.get_text()) for s in row.find_all('span')]
    entry = {'rank': labels[0] if labels else '', 'title': clean(title_el.get_text()),
             'artist': clean(artist_el.get_text()) if artist_el is not None else ''}
    seen = {}
    for i, text in enumerate(labels[:-1]):
        if text.upper() in LABELS:
            seen.setdefault(LABELS[text.upper()], set()).add(labels[i + 1])
    for column in LABELS.values():
        values = seen.get(column, set())
        if len(values) > 1:
            entry['problem'] = f'{column} differs between the two stat blocks: {sorted(values)}'
        value = next(iter(values), '')
        entry[column] = '' if value == '-' else value
    words = {re.sub(r'[\s-]+', '', t).upper() for t in labels}
    entry['status'] = 're-entry' if 'REENTRY' in words else 'new' if 'NEW' in words else ''
    entry['spans'] = labels
    return entry


def describe(soup, rows):
    # what the page looked like, for fixing the parser when a check fails
    lines = [f'  rows (ul.o-chart-results-list-row): {len(rows)}',
             f'  h3#title-of-a-story: {len(soup.find_all("h3", id="title-of-a-story"))}',
             f'  #chart-date-picker: {soup.find(id="chart-date-picker") is not None}']
    first_title = soup.find('h3', id='title-of-a-story')
    if not rows and first_title is not None:
        ancestors = [f'{p.name}.{".".join(p.get("class", []))}' for p in first_title.parents][:6]
        lines.append(f'  first title {clean(first_title.get_text())!r} sits in: {ancestors}')
    for row in rows[:3]:
        lines.append(f'  sample row spans: {[clean(s.get_text()) for s in row.find_all("span")][:20]}')
        lines.append(f'  sample row h3: {[clean(h.get_text()) for h in row.find_all("h3")]}')
    return '\n'.join(lines)


def parse(html):
    soup = BeautifulSoup(html, 'lxml')
    rows = soup.find_all('ul', class_='o-chart-results-list-row')
    chart_date, date_source = chart_date_of(soup, html)
    problems = []
    if chart_date is None:
        problems.append('no chart date found')
    entries = [e for e in (parse_row(r) for r in rows) if e is not None]
    if len(entries) != 100:
        problems.append(f'{len(entries)} songs found, expected 100')
    integer = re.compile(r'\d+')
    for e in entries:
        where = f'row {e["rank"]!r} ({e["title"]} by {e["artist"]})'
        if 'problem' in e:
            problems.append(f'{where}: {e["problem"]}')
        missing = [c for c in ('rank', 'peak_position', 'weeks_on_chart') if not integer.fullmatch(e[c])]
        if missing or (e['last_week'] and not integer.fullmatch(e['last_week'])) \
                or (e['weeks_at_no_1'] and not integer.fullmatch(e['weeks_at_no_1'])):
            problems.append(f'{where}: missing or non-numeric {missing or "last week / weeks at no. 1"}, '
                            f'spans {e["spans"][:20]}')
            continue
        rank, peak, weeks = int(e['rank']), int(e['peak_position']), int(e['weeks_on_chart'])
        if not e['title'] or not e['artist']:
            problems.append(f'{where}: missing title or artist')
        if peak > rank or (e['last_week'] and peak > int(e['last_week'])) or weeks < 1:
            problems.append(f'{where}: inconsistent last week / peak / weeks, spans {e["spans"][:20]}')
        if bool(e['weeks_at_no_1']) != (peak == 1):
            problems.append(f'{where}: peak {peak} but weeks at no. 1 is {e["weeks_at_no_1"]!r}')
        if e['last_week'] == '' and weeks > 1 and e['status'] != 're-entry':
            # a song with no last-week rank and more than one week must be re-entering
            problems.append(f'{where}: no last-week rank but {weeks} weeks and no RE-ENTRY badge, '
                            f'spans {e["spans"][:20]}')
    if not problems and [int(e['rank']) for e in entries] != list(range(1, 101)):
        problems.append(f'ranks are not 1..100 in order: {[e["rank"] for e in entries][:10]}...')
    if problems:
        raise RuntimeError('Hot 100 page did not parse as expected:\n  - '
                           + '\n  - '.join(problems[:10]) + '\nWhat the page had:\n' + describe(soup, rows))
    df = pd.DataFrame(entries)
    df.insert(0, 'chart_date', chart_date.isoformat())
    return df[COLUMNS], chart_date, date_source


parse_file = os.environ.get('PARSE_FILE')
dry_run = DRY_RUN or bool(parse_file)
if parse_file:
    with open(parse_file, encoding='utf-8') as f:
        html = f.read()
else:
    response = fetch(URL)
    if 'text/html' not in response.headers.get('Content-Type', ''):
        raise RuntimeError(f'unexpected Content-Type {response.headers.get("Content-Type")}')
    response.encoding = 'utf-8'
    html = response.text

df, chart_date, date_source = parse(html)
# the chart is dated a Saturday; a stale date means the page stopped updating (or the date is wrong)
if chart_date.weekday() != 5:
    raise RuntimeError(f'chart date {chart_date} (from {date_source}) is not a Saturday')
age = RUN_TIME.date() - chart_date
if age > timedelta(days=21) or age < timedelta(days=-14):
    raise RuntimeError(f'chart date {chart_date} (from {date_source}) is {age.days} days from today')

name = 'billboard-hot-100-' + chart_date.isoformat()
csv_key = PREFIX + name + '.csv'
body = df.to_csv(index=False).encode('utf-8')
summary = (f'chart dated {chart_date}: #1 {df.iloc[0]["title"]} by {df.iloc[0]["artist"]}, '
           f'{(df["status"] == "new").sum()} new, {(df["status"] == "re-entry").sum()} re-entries')

if parse_file:
    print(df.head(10).to_string(index=False))
    print(f'Parsed {summary}; not saved (PARSE_FILE is set)')
elif not dry_run and object_exists(csv_key):
    print(f'{csv_key} already saved; Billboard has not published a new chart since. Skipping.')
else:
    if dry_run:
        print(df.head(10).to_string(index=False))
    # a dry run skips the S3 check above and writes the CSV under ./out/
    metadata = {'source-url': URL, 'chart-date': chart_date.isoformat(), 'downloaded-at': RUN_DATETIME}
    save_bytes(body, csv_key, content_type='text/csv', metadata=metadata, label=summary)
