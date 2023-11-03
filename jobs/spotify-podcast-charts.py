import requests
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO
import os

# get environmental variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# get current date and datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
categories = ['top', 'arts', 'business', 'comedy', 'education', 'fiction', 'health%252520%2526%252520fitness', 'history', 'leisure', 'music', 'news', 'religion%252520%2526%252520spirituality', 'science', 'society%252520%2526%252520culture', 'sports', 'technology', 'true%252520crime', 'tv%252520%2526%252520film']

# define function
def get_podcast_chart(category):
    url = 'https://podcastcharts.byspotify.com/api/charts/' + category + '?region=us'
    #print(url)
    r = requests.get(url)
    rows = []
    for i in r.json():
        chart_rank_move = i['chartRankMove']
        show_description = i['showDescription']
        show_image_url = i['showImageUrl']
        show_name = i['showName']
        show_publisher = i['showPublisher']
        show_uri = i['showUri']
        row = [chart_rank_move, show_description, show_image_url, show_name, show_publisher, show_uri, category, url, current_datetime_label]
        rows.append(row)
    df = pd.DataFrame(rows)
    df.columns =['chart_rank_move', 'show_description', 'show_image_url', 'show_name', 'show_publisher', 'show_uri', 'category', 'url', 'datetime']
    df = df.reset_index()
    return(df)
   
# loop over categories
dfs = []
for category in categories:
    df = get_podcast_chart(category)
    dfs.append(df)
df = pd.concat(dfs)

# copy to s3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

file_path = 'spotify-podcast-charts/spotify-podcast-charts-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath=file_path)
