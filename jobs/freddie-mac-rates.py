from bs4 import BeautifulSoup
import pandas as pd

from common import RUN_DATETIME, RUN_LABEL, get, save_csv

# get page content
url = 'http://www.freddiemac.com/'
page = get(url)
soup = BeautifulSoup(page.content, "html.parser")

# extract values
items = []
for grid in soup.find_all(class_="rate grid-x"):
    name = grid.find(class_="name").text
    rate = grid.find(class_="rate-percent").text
    try:
        fees = grid.find(class_="fees").text
    except:
        fees = "Fee not displayed"
    item = [name, rate, fees, RUN_DATETIME]
    items.append(item)

# save as dataframe
df = pd.DataFrame(items)
df.columns = ['name', 'rate', 'fees', 'datetime']

file_path = 'freddie-mac-rates/freddie-mac-rates-' + RUN_LABEL + '.csv'
save_csv(df, file_path)
