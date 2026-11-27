import pandas as pd
import os

from common import RUN_DATETIME, RUN_LABEL, get_json, save_csv

# get environmental variables
NYC_OPEN_DATA_APP_TOKEN = os.environ['NYC_OPEN_DATA_APP_TOKEN']

# The URL of the API endpoint
url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

# Parameters to be sent in the query string; the app token goes in a header
# so it can't show up in an error message
params = {
    "$limit": 5000,
}

# Performing the GET request (fails loudly if it doesn't succeed)
data = get_json(url, params=params, headers={"X-App-Token": NYC_OPEN_DATA_APP_TOKEN}, timeout=120, name="nyc 311")
if not isinstance(data, list) or not data:
    raise SystemExit(f"Expected a non-empty list of rows, got: {str(data)[:200]}")

# Load the data into a Pandas DataFrame
df = pd.DataFrame(data)
df['scraped_at'] = RUN_DATETIME

# copy to s3
file_path = 'nyc-311/nyc-311-' + RUN_LABEL + '.csv'
save_csv(df, file_path)
