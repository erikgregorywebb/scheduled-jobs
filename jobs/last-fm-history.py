# import libraries
import pandas as pd
import os
from xml.etree import ElementTree as ET

from common import RUN_LABEL, get, save_csv

# set creds
API_KEY = os.environ['LAST_FM_API_KEY']
USER_NAME = 'erikgregorywebb'

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
        response = get("https://ws.audioscrobbler.com/2.0/", params=data, name=f'Last.fm page {page}')

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

file_path = 'last-fm/last-fm-history-' + RUN_LABEL + '.csv'
save_csv(df, file_path)
