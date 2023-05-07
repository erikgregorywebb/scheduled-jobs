# import packages
library(tidyverse)
library(httr)
library(jsonlite)
library(aws.s3)

# get current datetime
datetime = Sys.time()

# define headers
headers = c(
  `authority` = "api-searching.azurewebsites.net",
  `accept` = "application/json, text/plain, */*",
  `accept-language` = "en-US,en;q=0.9",
  `content-type` = "application/json",
  `dnt` = "1",
  `origin` = "https://platform.homie.com",
  `referer` = "https://platform.homie.com/",
  `request-context` = "appId=cid-v1:web-search",
  `request-id` = "|f3221629f9ca45fe9180fbf40567218a.e983c49ba3c6427e",
  `sec-ch-ua` = '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
  `sec-ch-ua-mobile` = "?0",
  `sec-ch-ua-platform` = '"macOS"',
  `sec-fetch-dest` = "empty",
  `sec-fetch-mode` = "cors",
  `sec-fetch-site` = "cross-site",
  `traceparent` = "00-f3221629f9ca45fe9180fbf40567218a-e983c49ba3c6427e-01",
  `user-agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
  `x-homie-appversion` = "WebSearch;2.0.0-37"
)

# define zips
zip_84057 = '{"geographicShapeWkt":"POLYGON ((-112.30592064430215 40.260127109668865, -111.11939720680215 40.260127109668865, -111.11939720680215 40.365367449920385, -112.30592064430215 40.365367449920385, -112.30592064430215 40.260127109668865))","page":1,"pageSize":500,"sortBy":"-publishDate"}'
zip_84058 = '{"geographicShapeWkt":"POLYGON ((-112.94110442854969 40.178870557214, -110.56805755354969 40.178870557214, -110.56805755354969 40.38944011114548, -112.94110442854969 40.38944011114548, -112.94110442854969 40.178870557214))","page":1,"pageSize":500,"sortBy":"-publishDate"}'
zip_84059 = '{"geographicShapeWkt":"POLYGON ((-112.345895204217 40.25406141283645, -111.159371766717 40.25406141283645, -111.159371766717 40.3593111980107, -112.345895204217 40.3593111980107, -112.345895204217 40.25406141283645))","page":1,"pageSize":500,"sortBy":"-publishDate"}'
zips = c(zip_84057, zip_84058, zip_84059)

# loop over zip list
datalist = list()
for (i in 1:length(zips)) {
  Sys.sleep(4)
  res <- httr::POST(url = "https://api-searching.azurewebsites.net/api/searching/v1/active-listings", httr::add_headers(.headers=headers), body = zips[i])
  content = rawToChar(res$content) %>% fromJSON()
  df = tibble(content$data)
  print(nrow(df))
  datalist[[i]] = df
}
raw = do.call(rbind, datalist) %>% distinct()

# export
writeToS3 = function(file,bucket,filename){
  s3write_using(file, FUN = write.csv, bucket = bucket, object = filename)
}
writeToS3(raw, 'egw-data-dumps', paste('homie/homie-export-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
