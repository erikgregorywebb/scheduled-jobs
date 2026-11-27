# Temporary probe (branch only): structure of a fundraiser page's Apollo state.
import re, json, requests
H = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36'}
h = requests.get('https://www.gofundme.com/discover', headers=H, timeout=30).text
slugs = list(dict.fromkeys(re.findall(r'/f/([a-z0-9][a-z0-9-]+)', h)))
print('featured slugs in order:', slugs)
r = requests.get(f'https://www.gofundme.com/f/{slugs[0]}', headers=H, timeout=30)
d = json.loads(re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S).group(1))
def find(o, key, path='$'):
    if isinstance(o, dict):
        for k, v in o.items():
            if k == key: yield path + '.' + k, v
            yield from find(v, key, path + '.' + k)
    elif isinstance(o, list):
        for i, x in enumerate(o): yield from find(x, key, f'{path}[{i}]')
for path, st in find(d, '__APOLLO_STATE__'):
    print('APOLLO at', path, 'entries', len(st))
    print('entry key prefixes:', sorted({k.split(':')[0] for k in st})[:40])
    for k, v in st.items():
        if isinstance(v, dict) and ('donationCount' in v or 'goalAmount' in v):
            print('\nFUNDRAISER ENTRY', k)
            for fk, fv in v.items(): print(f'   {fk}: {json.dumps(fv)[:160]}')
            break
