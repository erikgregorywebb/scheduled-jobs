# Temporary probe for alternative data sources (branch only, not for main).
import re, json, requests
BROWSER = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.9'}

def walk(o, path='$', out=None):
    """Find lists of dicts that look like fundraisers."""
    out = [] if out is None else out
    if isinstance(o, dict):
        for k, v in o.items(): walk(v, f'{path}.{k}', out)
    elif isinstance(o, list):
        if o and all(isinstance(x, dict) for x in o[:3]):
            keys = set().union(*[set(x.keys()) for x in o[:3]])
            if keys & {'slug', 'fundName', 'fund_name', 'title', 'url', 'goalAmount', 'currentAmount', 'donationCount'}:
                out.append((path, len(o), sorted(keys)))
        for i, x in enumerate(o[:50]): walk(x, f'{path}[{i}]', out)
    return out

print('===== GOFUNDME: robots.txt rules for all agents =====')
robots = requests.get('https://www.gofundme.com/robots.txt', headers=BROWSER, timeout=30).text
block = False
for line in robots.splitlines():
    l = line.strip()
    if l.lower().startswith('user-agent'): block = (l.split(':', 1)[1].strip() == '*')
    elif block and l.lower().startswith(('disallow', 'allow')): print('  ', l)

print('\n===== GOFUNDME: default vs browser User-Agent =====')
for name, hdrs in [('python default', {}), ('R/httr-like', {'User-Agent': 'libcurl/8.0 r-curl/5.2 httr/1.4.7'}), ('browser', BROWSER)]:
    r = requests.get('https://www.gofundme.com/discover/trending', headers=hdrs, timeout=30)
    print(f'  {name:15} status {r.status_code} bytes {len(r.text)} __NEXT_DATA__ {"__NEXT_DATA__" in r.text}')

print('\n===== GOFUNDME: fundraiser lists inside __NEXT_DATA__ on /discover/trending =====')
h = requests.get('https://www.gofundme.com/discover/trending', headers=BROWSER, timeout=30).text
d = json.loads(re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', h, re.S).group(1))
found = walk(d)
for path, n, keys in found[:15]: print(f'  {path}  ({n} items)\n     keys: {keys[:40]}')
if found:
    path = found[0][0]
    obj = d
    for part in re.findall(r'\.([^.\[]+)|\[(\d+)\]', path):
        obj = obj[part[0]] if part[0] else obj[int(part[1])]
    print('\n  first item of', path, ':\n  ', json.dumps(obj[0])[:1200])

print('\n\n===== TRAVEL ADVISORIES: official feeds =====')
for u in ['https://travel.state.gov/_res/rss/TAsTWs.xml', 'https://cadataapi.state.gov/api/TravelAdvisories',
          'https://travel.state.gov/content/travel/en/traveladvisories/traveladvisories.html/']:
    try:
        r = requests.get(u, headers=BROWSER, timeout=30)
        print(f'{u}\n  status {r.status_code} bytes {len(r.text)} type {r.headers.get("content-type")}')
        t = r.text
        if 'rss' in u:
            items = re.findall(r'<item>(.*?)</item>', t, re.S)
            print('  items:', len(items))
            for it in items[:2]: print('   ', re.sub(r'\s+', ' ', it)[:500])
        elif 'cadataapi' in u:
            try:
                j = r.json(); print('  json type', type(j).__name__, 'len', len(j)); print('  first:', json.dumps(j[0] if isinstance(j, list) else j)[:700])
            except Exception as e: print('  not json:', t[:200])
        else:
            print('  <table> count:', t.count('<table'), ' "Level" mentions:', t.count('Level '), ' title:', (re.search(r'<title>(.*?)</title>', t, re.S) or [None, None])[1])
    except Exception as e:
        print(f'{u}\n  ERROR {e}')
