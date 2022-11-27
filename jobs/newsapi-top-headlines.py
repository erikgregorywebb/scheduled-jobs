import requests
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO
import os

# get environmental variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
NEWSAPI_KEY = os.environ['NEWSAPI_KEY']

# get current date and datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# get headlines
# https://newsapi.org/docs/endpoints/top-headlines
url = 'https://newsapi.org/v2/top-headlines?country=us&apiKey={0}&pageSize=100'.format(NEWSAPI_KEY)
response = requests.get(url)
content = response.json()

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
    rows.append([source_id, source_name, author, title, description, url, urlToImage, publishedAt, content, current_datetime])
    
# create dataframe
df = pd.DataFrame(rows)
df.columns = ['source_id', 'source_name', 'author', 'title', 'description', 'url', 'urlToImage', 'publishedAt', 'content', 'datetime']

# copy to s3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

file_path = 'newsapi-top-headlines/newsapi-top-headlines-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath=file_path)
