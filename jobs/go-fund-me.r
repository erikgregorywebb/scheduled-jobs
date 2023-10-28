# import packages
library(tidyverse)
library(rvest)
library(dplyr)
library(httr)
library(jsonlite)
library(aws.s3)

# get current datetime
datetime = Sys.time()

# get top fundraisers
url = 'https://www.gofundme.com/discover'
page = read_html(url)
top_fundraisers = page %>% html_nodes('#campaign_click_on_discover_page') %>% html_nodes('a') %>% html_attr('href')

# get fundraiser details
datalist = list()
for (i in 1:length(top_fundraisers)) {
  Sys.sleep(1)
  url = top_fundraisers[i]
  print(url)
  page = read_html(url) 
  title = page %>% html_node('.p-campaign-title') %>% html_text2()
  raised_amount_goal = page %>% html_node('.progress-meter_progressMeterHeading__7dug0') %>% html_text2()
  donation_count_raw = page %>% html_node('.progress-meter_progressMeter__ebbGu') %>% html_text2()
  created_date = page %>% html_node('.a-created-date') %>% html_text2()
  description = page %>% html_node('.o-campaign-description') %>% html_text2()
  organizer = page %>% html_nodes('.m-campaign-byline-description') %>% html_text2()
  datalist[[i]] = tibble(rank = i, title = title, raised_amount_goal = raised_amount_goal, organizer = organizer,
                         donation_count_raw = donation_count_raw, created_date = created_date, description = description, url = url, datetime = datetime)
}
raw = do.call(rbind, datalist)

####### EXPORT ####### 
writeToS3 = function(file,bucket,filename){
  s3write_using(file, FUN = write.csv, bucket = bucket, object = filename)
}
writeToS3(raw, 'egw-data-dumps', paste('go-fund-me/go-fund-me-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
