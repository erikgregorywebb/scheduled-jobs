import time

import pandas as pd

from common import RUN_DATETIME, RUN_LABEL, post, save_csv

# KSL Homes' rental search, through the GraphQL API its search page calls
URL = 'https://homes.ksl.com/rent/api/graphql'
HEADERS = {
    'accept': '*/*',
    'content-type': 'application/json',
    'origin': 'https://homes.ksl.com',
    'referer': 'https://homes.ksl.com/rent/search/ut/provo',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
}
CITIES = ['Orem', 'Provo', 'Springville', 'Lehi']

# listing fields, in file order (contactMethod is requested but not saved)
FIELDS = ['addressSlug', 'bathrooms', 'bedrooms', 'contactMethod', 'contactName', 'contactPhone', 'contactSms',
          'contactEmail', 'fullAddress', 'id', 'lat', 'lon', 'price', 'primaryImage', 'squareFeet', 'title',
          'address', 'city', 'state', 'zip', 'propertyType', 'listingUrl', 'isFeatured', 'memberId', 'ribbonText',
          'hasRibbon', 'favoritedByMe', 'favoriteCount', 'propertyId', 'isDirectSold', '__typename']
QUERY = ('query searchListings($filter: ListingsFilter, $sort: String) {\n  listings(filter: $filter, sort: $sort) {\n'
         '    totalCount\n    hasMorePages\n    edges {\n      node {\n'
         + ''.join(f'        {f}\n' for f in FIELDS) +
         '      }\n      ecommerce\n      __typename\n    }\n    dataLayer\n    __typename\n  }\n}\n')
FILTER_KEYS = ['county', 'zip', 'minBedrooms', 'maxBedrooms', 'minBathrooms', 'maxBathrooms', 'minPrice', 'maxPrice',
               'minSquareFeet', 'maxSquareFeet', 'minAcres', 'maxAcres', 'minYearBuilt', 'maxYearBuilt',
               'minLeaseLength', 'maxLeaseLength', 'upToLeaseLength', 'allowSmoking', 'allowCats', 'allowDogs',
               'bottomLeftCoordinates', 'upperRightCoordinates']

frames = []
for city in CITIES:
    time.sleep(5)
    print(city)
    filter_ = {'limit': 250, 'city': city, 'state': 'UT', 'propertyType': [], 'amenities': [], 'communityAmenities': [],
               **{k: None for k in FILTER_KEYS}}
    body = {'operationName': 'searchListings', 'variables': {'filter': filter_, 'sort': ''}, 'query': QUERY}
    content = post(URL, headers=HEADERS, json=body, timeout=60, name=f'ksl {city}').json()
    if content.get('errors') or not (content.get('data') or {}).get('listings'):
        raise RuntimeError(f'KSL search for {city} returned no listings data: {str(content)[:300]}')
    nodes = [edge['node'] for edge in content['data']['listings']['edges']]
    print(f'  {len(nodes)} listings')
    frame = pd.DataFrame(nodes, columns=FIELDS).drop(columns='contactMethod')
    frame['inputCity'] = city  # the city searched (the R version mistakenly copied the listing's own city column)
    frame['scrapedAt'] = RUN_DATETIME
    frames.append(frame)
df = pd.concat(frames, ignore_index=True)
if df.empty:
    raise RuntimeError('KSL returned no rental listings in any city')

# booleans as TRUE/FALSE, as the R version wrote them
for col in ['isFeatured', 'hasRibbon', 'favoritedByMe', 'isDirectSold']:
    df[col] = df[col].map({True: 'TRUE', False: 'FALSE'})

# whole numbers as 2 rather than 2.0 (pandas makes a column float when any
# value is missing or fractional), as the R version wrote them
for col in ['bathrooms', 'bedrooms', 'price', 'squareFeet', 'favoriteCount']:
    df[col] = df[col].map(lambda x: None if pd.isna(x) else (f'{int(x)}' if float(x).is_integer() else f'{x}'))

save_csv(df, 'ksl/ksl-rentals-' + RUN_LABEL + '.csv')
