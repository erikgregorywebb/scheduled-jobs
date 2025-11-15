import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO
import os 
from urllib.parse import urljoin

# get current date and datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# URL for iPhone Finance charts
url = 'https://apps.apple.com/us/iphone/charts/6015'

# fetch page (spoof a normal browser)
headers = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/129.0.0.0 Safari/537.36"
    )
}
resp = requests.get(url, headers=headers)
resp.raise_for_status()

soup = BeautifulSoup(resp.text, "html.parser")

items = []

for a in soup.find_all("a", href=True):
    href = a["href"]

    # keep only app detail links
    if "/app/" not in href:
        continue

    label = a.get_text(" ", strip=True)

    # skip junky labels
    if not label:
        continue
    if label.lower() in ("view", "open", "download on the app store"):
        continue

    # make link absolute
    link = urljoin("https://apps.apple.com", href)

    items.append([label, link, current_datetime])

# build dataframe
df = pd.DataFrame(items, columns=["label", "link", "scraped_at"])

# drop duplicates (same app link & label)
df = df.drop_duplicates(subset=["label", "link"]).reset_index(drop=True)

print(df.head())
print(f"Scraped {len(df)} rows.")

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
