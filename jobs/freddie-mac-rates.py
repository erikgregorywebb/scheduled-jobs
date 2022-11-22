import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO
import os

# get page content
url = 'http://www.freddiemac.com/'
page = requests.get(url)
soup = BeautifulSoup(page.content, "html.parser")

# get current date and datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# extract values
items = []
for grid in soup.find_all(class_="rate grid-x"):
    name = grid.find(class_="name").text
    rate = grid.find(class_="rate-percent").text
    try:
        fees = grid.find(class_="fees").text
    except:
        fees = "Fee not displayed"
    item = [name, rate, fees, current_datetime]
    items.append(item)

# save as dataframe
df = pd.DataFrame(items)
df.columns = ['name', 'rate', 'fees', 'datetime']

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

file_path = 'freddie-mac-rates/freddie-mac-rates-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath=file_path)
