import time
import datetime as dt

import pandas as pd
import requests

from common import RUN_LABEL, RUN_TIME, get, save_csv

# -----------------------------------------------------------------------------
# Topviews settings
# -----------------------------------------------------------------------------
PROJECT = "en.wikipedia.org"
ACCESS = "all-access"  # matches Topviews "platform"
TOP_N = 1000
DAYS_BACK = 7
END_DATE = RUN_TIME.date() - dt.timedelta(days=1)  # yesterday in UTC (inclusive)

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

    try:
        resp = get(url, session=session, timeout=30, name=f"pageviews {day}")
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            print(f"  Data not available yet for {day}, skipping.")
            continue
        raise
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

if not rows:
    print("No data available for any day, exiting.")
    exit(0)
df = pd.DataFrame(rows, columns=["date", "rank", "page", "pageviews"])
df = df.sort_values(["date", "rank"], ascending=[False, True]).reset_index(drop=True)

file_path = 'wikipedia/daily-metrics/wikipedia-daily-metrics-' + RUN_LABEL + '.csv'
save_csv(df, file_path)
