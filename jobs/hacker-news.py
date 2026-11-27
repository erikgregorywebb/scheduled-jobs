import re
import time

import pandas as pd
from bs4 import BeautifulSoup

from common import RUN_DATETIME, RUN_LABEL, get, save_csv

# the first ten pages of Hacker News's front page, 30 stories each
URLS = [f'https://news.ycombinator.com/news?p={p}' for p in range(1, 11)]

def text(node):
    return node.get_text() if node is not None else None

rows = []
for url in URLS:
    time.sleep(15)  # HN asks crawlers to go slowly
    print(url)
    try:
        page = BeautifulSoup(get(url, timeout=30, name=url).text, 'lxml')
    except Exception as e:
        print(f'Skipping {url}: {e}')
        continue
    stories = page.select('.athing')
    subtexts = page.select('.subtext')
    for i, story in enumerate(stories):
        sub = subtexts[i] if i < len(subtexts) else None
        link = story.select_one('.titleline a')
        rank = text(story.select_one('.rank'))
        rank = re.search(r'\d+', rank) if rank else None
        comments = [a.get_text() for a in sub.select('a') if re.search('comment|discuss', a.get_text())] if sub else []
        rows.append({
            'id': int(story['id']) if story.get('id', '').isdigit() else None,
            'rank': int(rank.group(0)) if rank else None,
            'title': text(link),
            'link': link.get('href') if link is not None else None,
            'points': text(sub.select_one('.score')) if sub else None,
            'author': text(sub.select_one('a[href^="user?id="]')) if sub else None,
            'time_label': text(sub.select_one('.age a')) if sub else None,
            'comment_count': comments[-1] if comments else None,
            'source_url': url,
            'scraped_at': RUN_DATETIME,
        })
df = pd.DataFrame(rows)
if df.empty:
    raise RuntimeError('No Hacker News stories found; the page markup may have changed')
df['id'] = df['id'].astype('Int64')
df['rank'] = df['rank'].astype('Int64')

save_csv(df, 'hacker-news/hacker-news-' + RUN_LABEL + '.csv')
