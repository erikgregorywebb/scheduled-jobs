import os
import time
import datetime as dt
from datetime import datetime
from io import StringIO

import boto3
import pandas as pd
import requests

# -----------------------------------------------------------------------------
# Runtime timestamps
# -----------------------------------------------------------------------------
current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
current_datetime_label = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

# -----------------------------------------------------------------------------
# Environment variables (AWS)
# -----------------------------------------------------------------------------
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

# -----------------------------------------------------------------------------
# Topviews settings
# -----------------------------------------------------------------------------
PROJECT = "en.wikipedia.org"
ACCESS = "all-access"  # matches Topviews "platform"
TOP_N = 1000
DAYS_BACK = 7
END_DATE = dt.date.today() - dt.timedelta(days=1)  # yesterday (inclusive)

session = requests.Session()
session.headers.update({"User-Agent": "topviews-basic/1.0 (personal use)"})

rows = []

for i in range(DAYS_BACK):
    day = END_DATE - dt.timedelta(days=i)

    url = (
        "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/"
        f"{PROJECT}/{ACCESS}/{day:%Y}/{day:%m}/{day:%d}"
    )
    print(f"Fetching: {url}")

    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    articles = data["items"][0]["articles"][:TOP_N]
    for a in articles:
        rows.append(
            {
                "date": day.isoformat(),
                "rank": int(a["rank"]),
                "page": a["article"],
                "pageviews": int(a["views"]),
            }
        )

    time.sleep(0.1)  # be polite

df = pd.DataFrame(rows, columns=["date", "rank", "page", "pageviews"])
df = df.sort_values(["date", "rank"], ascending=[False, True]).reset_index(drop=True)

# -----------------------------------------------------------------------------
# Copy to S3
# -----------------------------------------------------------------------------
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)

    client.put_object(Bucket=bucket, Key=filepath, Body=csv_buf.getvalue())
    print(f"Copied {df.shape[0]:,} rows to s3://{bucket}/{filepath}")

file_path = f"wikipedia/wikipedia-daily-metrics-{current_datetime_label}.csv"
copy_to_s3(client=s3, df=df, bucket="egw-data-dumps", filepath=file_path)
