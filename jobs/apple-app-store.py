import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import boto3
import io

# get current date and datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# get page content
url = 'https://apps.apple.com/us/charts/iphone/finance-apps/6015?chart=top-free'
page = requests.get(url)
soup = BeautifulSoup(page.content, "html.parser")

# extract values
items = []
for item in soup.find_all(class_="we-lockup--in-app-shelf"):
    label = item.find('a')["aria-label"]
    link = item.find('a')["href"]
    item = [label, link, current_datetime]
    items.append(item)

# save as dataframe
df = pd.DataFrame(items)
df.columns = ['label', 'link', 'scraped_at']

# get environmental variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# copy to s3
# https://stackoverflow.com/questions/38154040/save-dataframe-to-csv-directly-to-s3-python
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

file_path = 'apple-app-store/apple-app-store-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath=file_path)
