# import packages
library(tidyverse)
library(rvest)
library(aws.s3)

# get current datetime
datetime = Sys.time()

# import parcel list
url = 'https://gist.githubusercontent.com/erikgregorywebb/741b472a8ae1bb97d617eda3d6aaea3d/raw/aef7b5083670659788a009185e0e055d0ac1b350/gila-buttes-parcel-search.csv'
parcels_df = read_csv(url)
parcels = parcels_df %>% pull(`PARCEL NUMBER`) %>% str_replace_all(., '-', '')

# scrape details
fields_list = list()
docs_list = list()
for (i in 1:length(parcels)) {
  Sys.sleep(.25)
  
  # read page
  url = paste('https://app1.pinal.gov/Search/Parcel-Details.aspx?parcel_ID=', parcels[i], sep = '')
  page = read_html(url)
  
  # grab fields
  parcel_number = page %>% html_node('#lblParcelNum') %>% html_text2()
  lot = page %>% html_node('#lblLot') %>% html_text2()
  owner_1 = page %>% html_node('#lblOwner1') %>% html_text2()
  owner_2 = page %>% html_node('#lblOwner2') %>% html_text2()
  in_care_of = page %>% html_node('#lblCareof') %>% html_text2()
  mailing_address = page %>% html_node('#lblMailingAddress') %>% html_text2()
  property_address = page %>% html_node('#lblPropAddress') %>% html_text2()
  recording_date = page %>% html_node('#lblRecDate') %>% html_text2()
  sale_amount = page %>% html_node('#lblSaleAmt') %>% html_text2()
  docs = page %>% html_node('.documents') %>% html_nodes('a') %>% html_attr('href')
  
  # throw into dataframe (fields)
  fields_temp = tibble(
    parcel_number = parcel_number,
    lot = lot,
    owner_1 = owner_1,
    owner_2 = owner_2,
    in_care_of = in_care_of,
    mailing_address = mailing_address,
    property_address = property_address,
    recording_date = recording_date,
    sale_amount = sale_amount,
    datetime = datetime
  )
  fields_list[[i]] = fields_temp
  
  # throw into dataframe (docs)
  docs_temp = tibble(
    parcel_number = parcel_number,
    doc_name = docs[!is.na(docs)] %>% basename(),
    doc_link = docs[!is.na(docs)],
    datetime = datetime
  )
  docs_list[[i]] = docs_temp
}
raw_fields = do.call(rbind, fields_list)
raw_docs = do.call(rbind, docs_list)

# export 
# https://medium.com/@som028/how-to-read-and-write-data-from-and-to-s3-bucket-using-r-3fed7e686844
#Sys.setenv("AWS_ACCESS_KEY_ID" = "", "AWS_SECRET_ACCESS_KEY" = "")
writeToS3 = function(file,bucket,filename){
  s3write_using(file, FUN = write.csv, bucket = bucket, object = filename)
}
writeToS3(raw_fields, 'egw-data-dumps', paste('gila-buttes/gila-buttes-fields-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
writeToS3(raw_docs, 'egw-data-dumps', paste('gila-buttes/gila-buttes-docs-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
