import requests
import pandas as pd

from common import RUN_LABEL, get, save_csv

categories = ['top', 'arts', 'business', 'comedy', 'education', 'fiction', 'health%252520%2526%252520fitness', 'history', 'leisure', 'music', 'news', 'religion%252520%2526%252520spirituality', 'science', 'society%252520%2526%252520culture', 'sports', 'technology', 'true%252520crime', 'tv%252520%2526%252520film']

# define function
def get_podcast_chart(category):
    url = 'https://podcastcharts.byspotify.com/api/charts/' + category + '?region=us'
    try:
        # one quick retry: a category can 500 for days (top did in Nov 2026), and
        # longer waits across 18 categories would outlast the step's timeout
        r = get(url, name=f'category {category}', delays=(5,))
    except (requests.HTTPError, RuntimeError) as e:
        print(f'Skipping category {category}: {e}')
        return None
    if r.status_code != 200 or not r.text.strip():
        print(f'Skipping category {category}: status {r.status_code}')
        return None
    rows = []
    for i in r.json():
        chart_rank_move = i['chartRankMove']
        show_description = i['showDescription']
        show_image_url = i['showImageUrl']
        show_name = i['showName']
        show_publisher = i['showPublisher']
        show_uri = i['showUri']
        row = [chart_rank_move, show_description, show_image_url, show_name, show_publisher, show_uri, category, url, RUN_LABEL]
        rows.append(row)
    df = pd.DataFrame(rows)
    df.columns =['chart_rank_move', 'show_description', 'show_image_url', 'show_name', 'show_publisher', 'show_uri', 'category', 'url', 'datetime']
    df = df.reset_index()
    return(df)

# loop over categories
dfs = []
for category in categories:
    df = get_podcast_chart(category)
    if df is not None:
        dfs.append(df)
if not dfs:
    print('No data scraped from any category, exiting.')
    exit(0)
df = pd.concat(dfs)

file_path = 'spotify-podcast-charts/spotify-podcast-charts-' + RUN_LABEL + '.csv'
save_csv(df, file_path)
