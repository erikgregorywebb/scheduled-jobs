library(httr)
library(tidyverse)
library(rvest)
library(jsonlite)
library(xml2)
library(aws.s3)

# get current datetime
datetime = Sys.time()

# Use a cookie-aware HTTP client for NYRR's waiting-room redirects.
url = 'https://www.nyrr.org/run/events-calendar'
response = GET(url, timeout(60), config(maxredirs = 10))
stop_for_status(response, task = 'fetch NYRR events calendar')
page = read_html(content(response, as = 'raw'))

# The calendar embeds its complete event list in Astro's serialized props.
calendar = page %>% html_node('astro-island[component-export="DynamicScheduleEventCards"]')
props_json = html_attr(calendar, 'props')
if (is.na(props_json)) {
  stop('NYRR calendar event data is missing; check for a waiting room or page redesign')
}
props = fromJSON(props_json, simplifyVector = FALSE)
# Astro encodes each value as [type, value]; events is an array of objects.
value = function(object, name) {
  field = object[[name]]
  if (length(field) < 2 || is.null(field[[2]])) return(NA_character_)
  as.character(field[[2]])
}
if (length(props$events) < 2) stop('NYRR calendar events are missing')
events = props$events[[2]]
nyrr = map_dfr(events, function(entry) {
  event = entry[[2]]
  if (!value(event, 'category') %in% c('races', 'virtual', 'youth')) return(NULL)
  start = as.POSIXct(value(event, 'startDateTime'), format = '%Y-%m-%dT%H:%M:%OSZ', tz = 'UTC')
  location = event$location[[2]]
  tibble(
    dates = format(start, '%Y-%m-%d', tz = 'America/New_York'),
    times = format(start, '%H:%M', tz = 'America/New_York'),
    titles = value(event, 'title'),
    statuses = value(event, 'registrationStatus'),
    locations = value(location, 'venue')
  )
})
if (nrow(nyrr) == 0 || any(is.na(nyrr$titles)) || any(is.na(nyrr$dates))) {
  stop('NYRR calendar returned no races or invalid race data; refusing to upload')
}
nyrr = nyrr %>% mutate(row_number = row_number(), extracted_at = datetime)

####### EXPORT ####### 
writeToS3 = function(file,bucket,filename){
  s3write_using(file, FUN = write.csv, bucket = bucket, object = filename)
  print(paste('Copy ', nrow(file), ' rows to S3 Bucket ', bucket, ' at ', filename, ' Done!'))
}
writeToS3(nyrr, 'egw-data-dumps', paste('nyrr/nyrr-2024-races-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
