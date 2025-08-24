library(tidyverse)
library(rvest)
library(lubridate)
library(httr)
library(jsonlite)
library(xml2)
library(aws.s3)

datetime = Sys.time()

# import
url = 'https://travel.state.gov/content/travel/en/traveladvisories/traveladvisories.html/'
page = read_html(url)
raw = page %>% html_table() %>% first()

# clean
table = raw %>%
  select(advisory = Destination, level = Level, date_updated = `Date Issued`) %>%
  mutate(date_updated = mdy(date_updated), scraped_at = datetime)

####### EXPORT ####### 
writeToS3 = function(file,bucket,filename){
  s3write_using(file, FUN = write.csv, bucket = bucket, object = filename)
  print(paste('Copy ', nrow(df), ' rows to S3 Bucket ', bucket, ' at ', filename, ' Done!'))
}
writeToS3(table, 'egw-data-dumps', paste('travel-advisories/travel-advisories-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
