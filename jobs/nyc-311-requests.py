import requests
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO
import os

# get environmental variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
NYC_OPEN_DATA_APP_TOKEN = os.environ['NYC_OPEN_DATA_APP_TOKEN']

# get current date and datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# The URL of the API endpoint
url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

# Parameters to be sent in the query string
params = {
    "$limit": 10000,
    "$$app_token": NYC_OPEN_DATA_APP_TOKEN
}

# Performing the GET request
response = requests.get(url, params=params)

# Checking if the request was successful
if response.status_code == 200:
    data = response.json()
    print(f"Retrieved {len(data)} records from the dataset!")

    # Load the data into a Pandas DataFrame
    df = pd.DataFrame(data)

    # Display the first few rows of the DataFrame
    print(df.head())
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")
    
df['scraped_at'] = current_datetime

# copy to s3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

file_path = 'nyc-311/nyc-311-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath=file_path)
