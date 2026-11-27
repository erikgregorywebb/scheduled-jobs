import requests
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
import boto3
from io import StringIO
import json
import time
import os

# Snapshots the FAA's live National Airspace System status: every airport or
# airspace currently under a ground stop, ground delay program, arrival/departure
# delay, airspace flow program, CTOP, or closure. The feed only shows what is in
# effect right now and is overwritten continuously, so these hourly snapshots
# are the history. Schema: https://nasstatus.faa.gov/AirportStatus.dtd
# robots.txt on nasstatus.faa.gov disallows nothing (checked 2026-11-27).

URL = 'https://nasstatus.faa.gov/api/airport-status-information'
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; scheduled-jobs data collector)'}

# columns always written, in this order, so files line up even when the feed
# has no restrictions or only some types; any other fields are appended after
COLUMNS = [
    'feed_update_time', 'delay_type', 'location', 'reason',
    'start', 'reopen', 'end_time',
    'avg', 'max',
    'arrival_min', 'arrival_max', 'arrival_trend',
    'departure_min', 'departure_max', 'departure_trend',
    'program_name', 'ctop_start_time', 'ctop_end_time',
    'fca_start_datetime', 'fca_end_datetime', 'afp_starttime', 'afp_endtime',
    'floor', 'ceiling', 'geometry', 'fca_data',
]
GEOMETRY = {'Line', 'Polygon', 'Circle'}

def element_to_json(element):
    # geometry and CTOP flow areas are nested; keep them whole as JSON
    node = dict(element.attrib)
    for child in element:
        node.setdefault(child.tag, []).append(element_to_json(child))
    if element.text and element.text.strip():
        node['text'] = element.text.strip()
    return {element.tag: node}

def flatten(item):
    row = {}
    for child in item:
        tag = child.tag
        if tag == 'Arrival_Departure':
            # up to two of these per airport, told apart by their Type attribute
            prefix = child.get('Type', 'unknown').lower()
            for leaf in child:
                row[f'{prefix}_{leaf.tag.lower()}'] = (leaf.text or '').strip()
        elif tag in GEOMETRY:
            row['geometry'] = json.dumps(element_to_json(child))
        elif tag == 'FCA_Data':
            row['fca_data'] = json.dumps(element_to_json(child))
        elif len(child):
            row[tag.lower()] = json.dumps(element_to_json(child))
        else:
            row[tag.lower()] = (child.text or '').strip()
    # one 'location' column: an airport code, or the airspace element for flow programs
    row['location'] = row.pop('arpt', None) or row.pop('ctl_element', None) or row.get('program_name')
    return row

# get the feed, retrying transient failures
for attempt in range(3):
    try:
        response = requests.get(URL, headers=HEADERS, timeout=30)
        response.raise_for_status()
        break
    except requests.RequestException as e:
        if attempt == 2:
            raise
        print(f'Attempt {attempt + 1} failed ({e}), retrying')
        time.sleep(15 * (attempt + 1))

root = ET.fromstring(response.content)
feed_update_time = root.findtext('Update_Time')

rows = []
for delay_type in root.findall('Delay_type'):
    name = delay_type.findtext('Name')
    for item_list in delay_type:
        if item_list.tag == 'Name':
            continue
        for item in item_list:
            row = flatten(item)
            row['delay_type'] = name
            row['feed_update_time'] = feed_update_time
            rows.append(row)

current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

df = pd.DataFrame(rows)
extra = [c for c in df.columns if c not in COLUMNS]
df = df.reindex(columns=COLUMNS + extra)
df['datetime'] = current_datetime
print(f'Feed updated {feed_update_time}: {len(df)} restrictions',
      df['delay_type'].value_counts().to_dict() if len(df) else '')

if os.environ.get('DRY_RUN'):
    # local check without touching S3: DRY_RUN=1 python jobs/faa-airport-status.py
    df.to_csv('faa-airport-status.csv', index=False)
    print('Dry run: wrote faa-airport-status.csv')
else:
    # get environmental variables
    AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

    # copy to s3; a file is written even with zero restrictions, so a quiet hour
    # can be told apart from a missed run
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    def copy_to_s3(client, df, bucket, filepath):
        csv_buf = StringIO()
        df.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)
        client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
        print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

    file_path = 'faa-airport-status/faa-airport-status-' + current_datetime_label + '.csv'
    copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath=file_path)
