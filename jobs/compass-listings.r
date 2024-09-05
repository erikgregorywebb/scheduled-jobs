# import packages
library(tidyverse)
library(rvest)
library(lubridate)
library(httr)
library(jsonlite)
library(xml2)
library(aws.s3)

datetime = Sys.time()

# define the header parameters
headers = c(
  accept = "*/*",
  `accept-language` = "en-US,en;q=0.9",
  `content-type` = "application/json",
  dnt = "1",
  origin = "https://www.compass.com",
  priority = "u=1, i",
  referer = "https://www.compass.com/for-rent/upper-west-side-manhattan-ny/sort=asc-price/",
  `sec-ch-ua` = '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
  `sec-ch-ua-mobile` = "?0",
  `sec-ch-ua-platform` = '"macOS"',
  `sec-fetch-dest` = "empty",
  `sec-fetch-mode` = "cors",
  `sec-fetch-site` = "same-origin",
  `user-agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)

# get the number of items in the search
data = '{"rawLolSearchQuery":{"listingTypes":[0],"rentalStatuses":[7,5,4,0,1],"num":1000,"sortOrder":2,"start":1,"facetFieldNames":["contributingDatasetList","compassListingTypes","comingSoon"]}, "purpose":"search"}'
res = httr::POST(url = 'https://www.compass.com/for-rent/upper-west-side-manhattan-ny/sort=asc-price/status=coming-soon,active,rented,application-pending,leases-out/start=1/', httr::add_headers(.headers=headers), body = data)
raw = fromJSON(rawToChar(res$content))
total_item_count = raw$lolResults$totalItems
pages = seq(from = 1, to = total_item_count, by = 40)

datalist = list()
for (i in 1:length(pages)) {
  Sys.sleep(30)
  
  # make call 
  print(paste(i, ' of ', length(pages), sep = ''))
  page_url = paste('https://www.compass.com/for-rent/upper-west-side-manhattan-ny/sort=asc-price/status=coming-soon,active,rented,application-pending,leases-out/start=', pages[i], '/', sep = '')
  data = sprintf('{"rawLolSearchQuery":{"listingTypes":[0],"rentalStatuses":[7,5,4,0,1],"num":1000,"sortOrder":%s,"start":1,"facetFieldNames":["contributingDatasetList","compassListingTypes","comingSoon"]}, "purpose":"search"}', pages[i])
  res = httr::POST(url = page_url, httr::add_headers(.headers=headers), body = data)
 
  # parse data
  raw = fromJSON(rawToChar(res$content))
  df_raw = raw$lolResults$data$listing %>% tibble() %>% mutate(page_url = page_url, page = pages[i])
  df = df_raw %>% 
    unnest(size, names_sep = '.') %>% 
    unnest(location, names_sep = '.') %>% 
    unnest(price, names_sep = '.') %>% 
    select(
      listing_id = listingIdSHA, 
      compass_property_id = compassPropertyId,
      listing_type = listingType, 
      pretty_address = location.prettyAddress, 
      street_number = location.streetNumber,
      street = location.street,
      unit_number = location.unitNumber, 
      unit_type = location.unitType, 
      neighborhood = location.neighborhood,
      city = location.city,
      state = location.state,
      zip_code = location.zipCode, 
      longitude = location.longitude,
      latitude = location.latitude,
      bedrooms = size.bedrooms,
      bathrooms = size.bathrooms,
      price_last_known = price.lastKnown,
      price_listed = price.listed, 
      scrape_page_url = page_url,
      scrape_page_number = page
    )
  datalist[[i]] = df
}
raw = do.call(rbind, datalist)

# clean
listings = raw %>%
  group_by(listing_id) %>%
  mutate(row_number = row_number()) %>% ungroup() %>%
  filter(row_number == 1) %>%
  mutate(street_order = parse_number(street)) %>%
  filter(!is.na(price_last_known)) %>%
  filter(!is.na(street_order)) %>%
  mutate(scraped_at = datetime)

# export
writeToS3 = function(file,bucket,filename){
  s3write_using(file, FUN = write.csv, bucket = bucket, object = filename)
  print(paste('Copy ', nrow(df), ' rows to S3 Bucket ', bucket, ' at ', filename, ' Done!'))
}
writeToS3(listings, 'egw-data-dumps', paste('compass/compass-listings-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
