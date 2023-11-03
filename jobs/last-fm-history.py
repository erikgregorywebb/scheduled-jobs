# import libraries
import requests
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO
import os
from xml.etree import ElementTree as ET

# set creds
API_KEY = os.environ['LAST_FM_API_KEY']
USER_NAME = 'erikgregorywebb'

# get current date and datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# define functions
def requestData(api_key, user, page=1):
    return {
        "method": "user.getrecenttracks",
        "user": user,
        "api_key": api_key,
        "limit": 200,
        "page": page
    }

def extractTracks(doc):
    root = ET.fromstring(doc)
    tracks = root.findall("./recenttracks/track")

    arr = []
    for track in tracks:
        track_data = {
            "artist": track.find("artist").text,
            "name": track.find("name").text,
            "album": track.find("album").text,
            "date": track.find("date").text if track.find("date") is not None else None
        }
        arr.append(track_data)

    return arr

def extractPageCount(doc):
    root = ET.fromstring(doc)
    recenttracks = root.find("./recenttracks")
    return int(recenttracks.get("totalPages"))

def fetch_all_tracks(api_key, user):
    page = 1
    all_tracks = []

    while True:
        data = requestData(api_key, user, page)
        response = requests.get("https://ws.audioscrobbler.com/2.0/", params=data)
        response.raise_for_status()

        tracks = extractTracks(response.content)
        all_tracks.extend(tracks)

        total_pages = extractPageCount(response.content)
        #if page >= total_pages:
        if page >= 10:
            break

        page += 1

    return all_tracks

tracks = fetch_all_tracks(API_KEY, USER_NAME)
df = pd.DataFrame(tracks)

# get environmental variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# copy to s3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

file_path = 'last-fm/last-fm-history-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath=file_path)
