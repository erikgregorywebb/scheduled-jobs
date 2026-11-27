from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin

from common import RUN_DATETIME, RUN_LABEL, get, save_csv

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
resp = get(url, headers=headers)

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

    items.append([label, link, RUN_DATETIME])

# build dataframe
df = pd.DataFrame(items, columns=["label", "link", "scraped_at"])

# drop duplicates (same app link & label)
df = df.drop_duplicates(subset=["label", "link"]).reset_index(drop=True)

print(df.head())
print(f"Scraped {len(df)} rows.")

file_path = 'apple-app-store/apple-app-store-' + RUN_LABEL + '.csv'
save_csv(df, file_path)
