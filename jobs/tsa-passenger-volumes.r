# import packages
library(tidyverse)
library(rvest)
library(aws.s3)

# get current datetime
datetime = Sys.time()

# import
url = 'https://www.tsa.gov/travel/passenger-volumes'
page = read_html(url)
table = page %>% html_table() %>% first()

# clean
df = table %>%
  pivot_longer(., !Date,  names_to = "year", values_to = "passenger_volume") %>%
  rename(date = Date) %>%
  mutate(passenger_volume = as.numeric(gsub(",", "", passenger_volume))) %>%
  mutate(scraped_at = datetime)

####### EXPORT ####### 
writeToS3 = function(file,bucket,filename){
  s3write_using(file, FUN = write.csv, bucket = bucket, object = filename)
}
writeToS3(df, 'egw-data-dumps', paste('tsa/tsa-passenger-volumes-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
