# Temporary probe (branch only, not for main): where is the fundraiser list on /discover?
import re, json, requests, collections
H = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36',
     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.9'}
for url in ['https://www.gofundme.com/discover', 'https://www.gofundme.com/discover/trending']:
    h = requests.get(url, headers=H, timeout=30).text
    print(f'===== {url}  ({len(h)} bytes)')
    for label, pat in [('/f/ plain', r'/f/[a-z0-9-]{4,}'), ('\\/f\\/ escaped', r'\\/f\\/[a-z0-9-]{4,}'), ('\\u002Ff\\u002F', r'\\u002[Ff]f\\u002[Ff][a-z0-9-]{4,}'),
                       ('gofund.me', r'gofund\.me/[A-Za-z0-9]+'), ('"slug":', r'"slug"\s*:\s*"[^"]+"'), ('"fundraiserSlug"', r'"[a-zA-Z]*[Ss]lug"\s*:\s*"[^"]+"')]:
        m = re.findall(pat, h); print(f'  {label:18} {len(m):4}  {sorted(set(m))[:4]}')
    nd = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', h, re.S)
    print('  __NEXT_DATA__ bytes:', len(nd.group(1)) if nd else None)
    d = json.loads(nd.group(1))
    keys = collections.Counter()
    def walk(o, path):
        if isinstance(o, dict):
            for k, v in o.items():
                if re.search(r'(?i)fund|slug|donat|goal|amount|campaign', k): keys[re.sub(r'\[\d+\]', '[]', f'{path}.{k}')] += 1
                walk(v, f'{path}.{k}')
        elif isinstance(o, list):
            for i, x in enumerate(o): walk(x, f'{path}[{i}]')
    walk(d, '$')
    print('  fundraiser-ish key paths in __NEXT_DATA__:')
    for k, n in keys.most_common(25): print(f'    {n:4}  {k[:160]}')
    fb = d['props']['pageProps'].get('__SWR_FALLBACK__', {})
    print('  __SWR_FALLBACK__ keys:', [k[:100] for k in list(fb)[:10]])
    ip = d['props']['pageProps'].get('initialPropsFromServer', {})
    print('  initialPropsFromServer keys:', list(ip)[:20] if isinstance(ip, dict) else type(ip))
    print('  other <script> JSON blobs:', [m[:60] for m in re.findall(r'<script[^>]+type="application/(?:json|ld\+json)"[^>]*id="([^"]*)"', h)][:10])
