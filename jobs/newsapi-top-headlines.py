import pandas as pd
import os

from common import RUN_DATETIME, RUN_LABEL, get_json, save_csv

# get environmental variables
NEWSAPI_KEY = os.environ['NEWSAPI_KEY']

# get headlines
# https://newsapi.org/docs/endpoints/top-headlines
# the key goes in a header rather than the URL, so it can't show up in an error message
content = get_json('https://newsapi.org/v2/top-headlines', params={'country': 'us', 'pageSize': 100},
                   headers={'X-Api-Key': NEWSAPI_KEY}, name='newsapi top-headlines')

# extract content 
rows = []
for article in content['articles']:
    source_id = article['source']['id']
    source_name = article['source']['name']
    author = article['author']
    title = article['title']
    description = article['description']
    url = article['url']
    urlToImage = article['urlToImage']
    publishedAt = article['publishedAt']
    content = article['content']
    rows.append([source_id, source_name, author, title, description, url, urlToImage, publishedAt, content, RUN_DATETIME])
    
# create dataframe
df = pd.DataFrame(rows)
df.columns = ['source_id', 'source_name', 'author', 'title', 'description', 'url', 'urlToImage', 'publishedAt', 'content', 'datetime']

# copy to s3
file_path = 'newsapi-top-headlines/newsapi-top-headlines-' + RUN_LABEL + '.csv'
save_csv(df, file_path)
