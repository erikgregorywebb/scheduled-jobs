# Temporary probe (branch only): what does Hacker News return to GitHub's runners?
import re, requests
from bs4 import BeautifulSoup
for name, hdrs in [('python default', {}), ('R/libcurl-like', {'User-Agent': 'libcurl/8.5.0 r-curl/6.0.1 httr/1.4.7'}), ('no user agent', {'User-Agent': ''}),
                   ('browser', {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36'})]:
    try:
        r = requests.get('https://news.ycombinator.com/news?p=1', headers=hdrs, timeout=30)
    except Exception as e:
        print(f'{name:16} ERROR {e}'); continue
    soup = BeautifulSoup(r.text, 'html.parser')
    t = soup.title.get_text(strip=True) if soup.title else None
    print(f'{name:16} status {r.status_code} bytes {len(r.text)} title {t!r}  .athing {len(soup.select(".athing"))}  .subtext {len(soup.select(".subtext"))}  .titleline {len(soup.select(".titleline"))}')
    if len(soup.select('.athing')) == 0:
        print('   body start:', re.sub(r'\s+', ' ', soup.get_text(' ', strip=True))[:300])
        print('   tr classes:', sorted({' '.join(tr.get('class', [])) for tr in soup.find_all('tr')})[:15])
r = requests.get('https://news.ycombinator.com/news?p=1', timeout=30)
soup = BeautifulSoup(r.text, 'html.parser')
row = soup.select_one('.athing')
if row:
    print('\nfirst story row HTML:\n', str(row)[:700])
    sub = soup.select_one('.subtext')
    print('\nfirst subtext HTML:\n', str(sub)[:700] if sub else None)
print('\nofficial API check:', requests.get('https://hacker-news.firebaseio.com/v0/topstories.json', timeout=30).json()[:5])
