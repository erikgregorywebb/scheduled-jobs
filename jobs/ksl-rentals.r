library(tidyverse)
library(rvest)
library(httr)
library(jsonlite)
library(xml2)
library(aws.s3)

datetime = Sys.time()

headers = c(
  `accept` = "*/*",
  `accept-language` = "en-US,en;q=0.9",
  `content-type` = "application/json",
  `dnt` = "1",
  `origin` = "https://homes.ksl.com",
  `priority` = "u=1, i",
  `referer` = "https://homes.ksl.com/rent/search/ut/provo",
  `sec-ch-ua` = '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
  `sec-ch-ua-mobile` = "?0",
  `sec-ch-ua-platform` = '"macOS"',
  `sec-fetch-dest` = "empty",
  `sec-fetch-mode` = "cors",
  `sec-fetch-site` = "same-origin",
  `user-agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

cities = c('Orem', 'Provo', 'Springville', 'Lehi')
datalist = list()
for (i in 1:length(cities)) {
  Sys.sleep(5)
  city = cities[i]
  print(city)
  data = sprintf('{"operationName":"searchListings","variables":{"filter":{"limit":250,"city":"%s","county":null,"state":"UT","zip":null,"minBedrooms":null,"maxBedrooms":null,"minBathrooms":null,"maxBathrooms":null,"minPrice":null,"maxPrice":null,"minSquareFeet":null,"maxSquareFeet":null,"minAcres":null,"maxAcres":null,"minYearBuilt":null,"maxYearBuilt":null,"minLeaseLength":null,"maxLeaseLength":null,"upToLeaseLength":null,"allowSmoking":null,"allowCats":null,"allowDogs":null,"propertyType":[],"amenities":[],"communityAmenities":[],"bottomLeftCoordinates":null,"upperRightCoordinates":null},"sort":""},"query":"query searchListings($filter: ListingsFilter, $sort: String) {\\n  listings(filter: $filter, sort: $sort) {\\n    totalCount\\n    hasMorePages\\n    edges {\\n      node {\\n        addressSlug\\n        bathrooms\\n        bedrooms\\n        contactMethod\\n        contactName\\n        contactPhone\\n        contactSms\\n        contactEmail\\n        fullAddress\\n        id\\n        lat\\n        lon\\n        price\\n        primaryImage\\n        squareFeet\\n        title\\n        address\\n        city\\n        state\\n        zip\\n        propertyType\\n        listingUrl\\n        isFeatured\\n        memberId\\n        ribbonText\\n        hasRibbon\\n        favoritedByMe\\n        favoriteCount\\n        propertyId\\n        isDirectSold\\n        __typename\\n      }\\n      ecommerce\\n      __typename\\n    }\\n    dataLayer\\n    __typename\\n  }\\n}\\n"}', city)
  res <- httr::POST(url = "https://homes.ksl.com/rent/api/graphql", httr::add_headers(.headers=headers), body = data)
  content = rawToChar(res$content) %>% fromJSON()
  temp = tibble(content$data$listings$edges$node) %>% select(-contactMethod) %>% 
    mutate(inputCity = city, scrapedAt = datetime)
  print(temp)
  datalist[[i]] = temp
}
raw = do.call(rbind, datalist)

####### EXPORT ####### 
writeToS3 = function(file,bucket,filename){
  s3write_using(file, FUN = write.csv, bucket = bucket, object = filename)
  print(paste('Copy ', nrow(df), ' rows to S3 Bucket ', bucket, ' at ', filename, ' Done!'))
}
writeToS3(raw, 'egw-data-dumps', paste('ksl/ksl-rentals-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
