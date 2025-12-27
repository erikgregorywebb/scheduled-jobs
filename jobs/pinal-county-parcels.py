import requests
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO
import os

import re
import time
import random
import requests
from bs4 import BeautifulSoup

# get environmental variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# get current date and datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

WANTED = [
    "Parcel NUMBER",
    "S/T/R",
    "PROPERTY DESCRIPTION",
    "SUBDIVISION",
    "UNIT",
    "BLOCK",
    "LOT",
    "PHASE",
    "CABINET",
    "SLIDE",
    "PRIMARY OWNER",
    "NAME 2",
    "IN C/O",
    "TAX BILL MAILING ADDRESS",
    "PROPERTY ADDRESS(LOCATION)",
    "PARCEL ALERT LIST",
    "DATE OF RECORDING",
    "SALE AMOUNT",
]

def _norm(s: str) -> str:
    s = (s or "").strip().upper()
    s = re.sub(r"\s+", " ", s)
    s = s.replace(" (", "(").replace("( ", "(")
    return s

def _extract_selected_from_html(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    lines = [x.strip() for x in soup.get_text("\n").split("\n")]
    lines = [x for x in lines if x]

    wanted_norm = {_norm(x): x for x in WANTED}
    out = {k: None for k in WANTED}

    i = 0
    while i < len(lines):
        key_norm = _norm(lines[i])
        if key_norm in wanted_norm:
            canonical = wanted_norm[key_norm]
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            out[canonical] = lines[j].strip() if j < len(lines) else None
            i = j + 1
        else:
            i += 1

    return out

def fetch_one(parcel_id, session: requests.Session, timeout=30) -> dict:
    url = f"https://app1.pinal.gov/Search/Parcel-Details.aspx?parcel_ID={parcel_id}"
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    fields = _extract_selected_from_html(r.text)
    fields["parcel_id"] = str(parcel_id)   # the query id you passed
    fields["source_url"] = url
    return fields

def fetch_many(parcel_ids, *, sleep_range=(0.15, 0.45), timeout=30, max_errors=25):
    """
    Returns:
      wide_df: one row per parcel_id, columns are WANTED + parcel_id + source_url
      errors_df: rows for failures (parcel_id, error)
    """
    rows = []
    errors = []

    with requests.Session() as s:
        s.headers.update({"User-Agent": "Mozilla/5.0"})

        for idx, pid in enumerate(parcel_ids, start=1):
            try:
                rows.append(fetch_one(pid, s, timeout=timeout))
            except Exception as e:
                errors.append({"parcel_id": str(pid), "error": repr(e)})
                if len(errors) >= max_errors:
                    break
            # be polite to the server
            time.sleep(random.uniform(*sleep_range))

    wide_df = pd.DataFrame(rows)

    # Ensure consistent column order
    col_order = ["parcel_id"] + WANTED + ["source_url"]
    for c in col_order:
        if c not in wide_df.columns:
            wide_df[c] = None
    wide_df = wide_df[col_order].sort_values("parcel_id").reset_index(drop=True)

    errors_df = pd.DataFrame(errors)
    return wide_df, errors_df

# ---- Example usage ----
parcel_ids = [
509843490,
509843500,
509843510,
509843520
]

wide_df, errors_df = fetch_many(parcel_ids)

# copy to s3
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

file_path = 'pinal-county-parcels/gila-buttes-parcels-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=wide_df, bucket='egw-data-dumps', filepath=file_path)
