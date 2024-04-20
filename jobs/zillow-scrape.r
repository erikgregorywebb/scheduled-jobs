# import packages
library(tidyverse)
library(dplyr)
library(httr)
library(jsonlite)
library(aws.s3)

# get current datetime
datetime = Sys.time()

# download
url = 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1693977204'
raw = read_csv(url)

####### EXPORT ####### 
writeToS3 = function(file,bucket,filename){
  s3write_using(file, FUN = write.csv, bucket = bucket, object = filename)
}
writeToS3(raw, 'egw-data-dumps', paste('zillow/zhvi-all-homes-zip-code-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
print(paste('job run: zillow/zhvi-all-homes-zip-code-' , format(datetime, '%Y-%m-%d-%H-%M-%S')))
