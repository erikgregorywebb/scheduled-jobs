# import libraries
import requests
import pandas as pd
import datetime
import boto3
from io import StringIO
import os

# set client id, client secret, and auth url
CLIENT_ID = os.environ['SPOTIFY_CLIENT_ID']
CLIENT_SECRET = os.environ['SPOTIFY_CLIENT_SECRET']
AUTH_URL = 'https://accounts.spotify.com/api/token'

# post for access token
auth_response = requests.post(AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
})

# convert the response to JSON, save the access token
auth_response_data = auth_response.json()
access_token = auth_response_data['access_token']

# define function
def get_playlist_tracks(playlist_name, playlist_id):

    # define headers
    headers = {'Authorization': 'Bearer {token}'.format(token=access_token)}

    # set base URL of all Spotify API endpoints
    BASE_URL = 'https://api.spotify.com/v1/'

    # GET request with proper header
    r = requests.get(BASE_URL + 'playlists/' + playlist_id + '/tracks', headers=headers)
    content = r.json()

    # get current datetime
    current_datetime = datetime.datetime.now()

    # extract data elements
    rows = []
    for i, item in enumerate(content['items']):
        track_playlist_position = i + 1
        track_name = item['track']['name']
        track_id = item['track']['id']
        track_href = item['track']['href']
        track_release_date = item['track']['album']['release_date']
        track_added_at = item['added_at']

        for j, artist in enumerate(item['track']['artists']):
            artist_track_position = j + 1
            artist_name = artist['name']
            artist_id = artist['id']
            artist_href = artist['href']

            row = [playlist_id, playlist_name, current_datetime, track_playlist_position, track_name, track_id, track_href, track_release_date, track_added_at,
                  artist_track_position, artist_name, artist_id, artist_href]
            rows.append(row)

    # create pandas dataframe
    playlist_tracks = pd.DataFrame(rows, columns = ['playlist_id', 'playlist_name', 'current_datetime', 'track_playlist_position',
                                                    'track_name', 'track_id', 'track_href', 'track_release_date', 'track_added_at',
                                                    'artist_track_position', 'artist_name', 'artist_id', 'artist_href'])
    return(playlist_tracks)

# get tracks of Rap Cavier playlist (https://open.spotify.com/playlist/37i9dQZF1DX0XUsuxWHRQd)
rap_cavier = get_playlist_tracks('Rap Cavier', '37i9dQZF1DX0XUsuxWHRQd')

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

file_path = 'spotify-playlist-history/rap-cavier-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=rap_cavier, bucket='egw-data-dumps', filepath=file_path)
