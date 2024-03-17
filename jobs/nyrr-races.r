library(httr)
library(tidyverse)
library(jsonlite)
library(xml2)
library(aws.s3)

# get current datetime
datetime = Sys.time()

# extract
url = 'https://www.nyrr.org/fullraceyearindex'
page = read_html(url)
listings = page %>% html_node('#indexlistings')
dates = listings %>% html_nodes('div.index_listing__date') %>% html_text2()
times = listings %>% html_nodes('div.index_listing__time') %>% html_text2()
titles = listings %>% html_nodes('div.index_listing__title') %>% html_text2()
locations = listings %>% html_nodes('div.index_listing__location') %>% html_text2()
statuses = listings %>% html_nodes('div.index_listing__status') %>% html_text2()

# combine, clean
nyrr = tibble(dates = dates, times = times, titles = trimws(titles), 
             statuses = trimws(statuses), locations = locations) %>%
  mutate(row_number = row_number(), extracted_at = datetime)

####### EXPORT ####### 
writeToS3 = function(file,bucket,filename){
  s3write_using(file, FUN = write.csv, bucket = bucket, object = filename)
  print(paste('Copy ', nrow(df), ' rows to S3 Bucket ', bucket, ' at ', filename, ' Done!'))
}
writeToS3(nyrr, 'egw-data-dumps', paste('mta/nyrr-2024-races-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
