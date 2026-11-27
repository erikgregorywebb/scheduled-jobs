# Temporary probe for alternative GoFundMe data sources (branch only, not for main).
import re, json, requests
UA = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.9'}
SLUG = re.compile(r'(?:gofundme\.com)?/f/([a-z0-9][a-z0-9-]{3,})')
def get(url, **kw):
    try:
        r = requests.get(url, headers=UA, timeout=30, **kw); return r
    except Exception as e:
        print(f'  ERROR {url}: {e}'); return None
def summarize(url):
    r = get(url)
    if r is None: return None, []
    h = r.text
    slugs = sorted(set(SLUG.findall(h)))
    flags = {k: (k.lower() in h.lower()) for k in ['__NEXT_DATA__', '__APOLLO_STATE__', 'graphql', 'algolia', 'application/ld+json', 'challenge-platform', 'cf-chl']}
    t = re.search(r'<title[^>]*>(.*?)</title>', h, re.S)
    print(f'{url}\n  status {r.status_code}  bytes {len(h)}  title {t.group(1).strip()[:60] if t else None!r}  slugs {len(slugs)}  final {r.url}')
    print('  ', {k: v for k, v in flags.items() if v})
    if slugs: print('   sample slugs:', slugs[:5])
    return h, slugs

print('===== robots.txt =====')
r = get('https://www.gofundme.com/robots.txt')
robots = r.text if r is not None else ''
print('status', r.status_code if r is not None else None)
for line in robots.splitlines():
    if re.match(r'(?i)\s*(user-agent|disallow|allow|sitemap)', line) and any(k in line.lower() for k in ['user-agent: *', '/f/', 'discover', '/s', 'sitemap', 'api', 'graphql']):
        print('  ', line.strip())

print('\n===== listing pages =====')
all_slugs = set(); discover_html = None
for u in ['https://www.gofundme.com/discover', 'https://www.gofundme.com/discover/trending', 'https://www.gofundme.com/discover/medical-fundraiser',
          'https://www.gofundme.com/s?q=help', 'https://www.gofundme.com/c/trending']:
    h, s = summarize(u); all_slugs |= set(s)
    if u.endswith('/discover'): discover_html = h

print('\n===== URLs referenced by the discover page (api/graphql/search) =====')
if discover_html:
    urls = sorted(set(re.findall(r'https?://[a-zA-Z0-9.-]+(?:/[^"\'\s<>\\)]*)?', discover_html)))
    for u in urls:
        if any(k in u.lower() for k in ['api', 'graphql', 'algolia', 'search', 'gateway']): print('  ', u[:150])
    nd = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', discover_html, re.S)
    if nd:
        d = json.loads(nd.group(1)); print('  __NEXT_DATA__ top keys:', list(d.keys()), ' page:', d.get('page'))
        print('  pageProps keys:', list(d.get('props', {}).get('pageProps', {}).keys())[:30])

print('\n===== sitemaps =====')
for sm in re.findall(r'(?i)sitemap:\s*(\S+)', robots)[:6]:
    r = get(sm)
    if r is None: continue
    locs = re.findall(r'<loc>(.*?)</loc>', r.text)
    print(f'{sm}  status {r.status_code}  entries {len(locs)}'); [print('   ', l) for l in locs[:6]]
    all_slugs |= set(SLUG.findall(r.text))

print('\n===== a fundraiser page =====')
print('total distinct slugs found anywhere:', len(all_slugs))
slug = sorted(all_slugs)[0] if all_slugs else None
if slug:
    u = f'https://www.gofundme.com/f/{slug}'
    h, _ = summarize(u)
    if h:
        for m in re.findall(r'<script type="application/ld\+json"[^>]*>(.*?)</script>', h, re.S)[:3]:
            try: j = json.loads(m); print('  ld+json:', json.dumps(j)[:400])
            except Exception: pass
        for k in ['og:title', 'og:description']:
            m = re.search(rf'<meta[^>]+property="{k}"[^>]+content="([^"]*)"', h); print(f'  {k}:', m.group(1)[:120] if m else None)
        for k in ['goalAmount', 'goal_amount', 'currentAmount', 'current_amount', 'donationCount', 'donation_count', 'donationsCount', 'createdAt', 'launch_date', 'fundName', 'fund_name']:
            m = re.search(rf'"{k}"\s*:\s*("?[^,}}]{{0,60}})', h)
            if m: print(f'  {k}: {m.group(1)}')
