# import packages
library(readr)
library(httr)
library(aws.s3)

# get current datetime
datetime = Sys.time()

url = 'https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1693977204'
bucket = 'egw-data-dumps'
prefix = 'zillow/'

####### SKIP IF UNCHANGED #######
# Zillow republishes this file about once a month. Compare when Zillow last
# changed it with our newest saved copy, and skip the download and upload if
# that copy is newer. When either date can't be read, save a copy as before.
source_modified = tryCatch({
  resp = HEAD(url, timeout(60))
  stop_for_status(resp)
  parse_http_date(headers(resp)[['last-modified']])
}, error = function(e) { message('could not read Zillow Last-Modified: ', conditionMessage(e)); NA })

latest_saved = tryCatch({
  saved = get_bucket_df(bucket, prefix = prefix, max = Inf)
  if (nrow(saved) == 0) NA else max(as.POSIXct(saved$LastModified, format = '%Y-%m-%dT%H:%M:%OSZ', tz = 'UTC'))
}, error = function(e) { message('could not list saved copies: ', conditionMessage(e)); NA })

print(paste('Zillow last modified:', format(source_modified, tz = 'UTC', usetz = TRUE),
            '| newest saved copy:', format(latest_saved, tz = 'UTC', usetz = TRUE)))

if (!is.na(source_modified) && !is.na(latest_saved) && latest_saved > source_modified) {
  print('job run: zillow file unchanged since last saved copy, skipping upload')
  quit(save = 'no', status = 0)
}

####### DOWNLOAD #######
raw = read_csv(url)

####### EXPORT #######
writeToS3 = function(file,bucket,filename){
  s3write_using(file, FUN = write.csv, bucket = bucket, object = filename)
}
writeToS3(raw, bucket, paste(prefix, 'zhvi-all-homes-zip-code-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
print(paste('job run: zillow/zhvi-all-homes-zip-code-' , format(datetime, '%Y-%m-%d-%H-%M-%S')))
