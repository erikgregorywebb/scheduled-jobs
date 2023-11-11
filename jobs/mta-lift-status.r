library(httr)
library(tidyverse)
library(jsonlite)
library(xml2)
library(aws.s3)

# get current datetime
datetime = Sys.time()

### METRO NORTH RAILRAOD

# define headers
headers = c(
  `authority` = "mnrtraintimeapi.mta.info",
  `accept` = "application/json, text/plain, */*",
  `accept-language` = "en-US,en;q=0.9",
  `dnt` = "1",
  `origin` = "https://new.mta.info",
  `referer` = "https://new.mta.info/elevator-escalator-status",
  `sec-ch-ua` = '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
  `sec-ch-ua-mobile` = "?0",
  `sec-ch-ua-platform` = '"macOS"',
  `sec-fetch-dest` = "empty",
  `sec-fetch-mode` = "cors",
  `sec-fetch-site` = "same-site",
  `user-agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
  `x-api-key` = "9ea83a8a361efacd098b6c7f6a6e49c1"
)

# make call
r <- httr::GET(url = "https://mnrtraintimeapi.mta.info/wsnk/ttapi/api/stations/lift-equipments", httr::add_headers(.headers=headers))
content = rawToChar(r$content) %>% fromJSON()

# extract station list
stations = tibble(station_id = content$StationId, station_name = content$StationName,
       branch_id = content$BranchId, branch_name = content$BranchName)

# extract status list, join to station lookup
elevators = tibble(do.call(rbind, content$Elevators)) %>% mutate(type = 'elevator')
escalators = tibble(do.call(rbind, content$Escalators)) %>% mutate(type = 'escalator')
mnr_lifts = rbind(elevators, escalators) %>%
  rename(station_id = StationId, station_name = StationName, station_number = StationNumber, lift_id = LiftId,
         description = Description, lift_type = LiftType, status_id = StatusId, status = Status, reported_date = ReportedDate) %>%
  left_join(x = ., y = stations %>% select(station_id, branch_id, branch_name), by = 'station_id')

### SUBWAY

# define headers
headers = c(
  `authority` = "api-endpoint.mta.info",
  `accept` = "application/json, text/plain, */*",
  `accept-language` = "en-US,en;q=0.9",
  `dnt` = "1",
  `origin` = "https://new.mta.info",
  `referer` = "https://new.mta.info/elevator-escalator-status",
  `sec-ch-ua` = '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
  `sec-ch-ua-mobile` = "?0",
  `sec-ch-ua-platform` = '"macOS"',
  `sec-fetch-dest` = "empty",
  `sec-fetch-mode` = "cors",
  `sec-fetch-site` = "same-site",
  `user-agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
  `x-api-key` = "KjuU3wmp257OhPmBHxrbu81pVTV0jOK3aVN0LnXu"
)

# make call
r <- httr::GET(url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2fnyct_ene_equipments.xml", httr::add_headers(.headers=headers))
rawToChar(r$content)

# read XML content
xml_data <- read_xml(rawToChar(r$content))

# extract equipment nodes
equipment_nodes <- xml_find_all(xml_data, ".//equipment")

# prase nodes into a list, convert list to dataframe
equipment_list <- map(equipment_nodes, function(node) {
  data <- xml_children(node)
  setNames(map_chr(data, xml_text), xml_name(data))
})
subway_lifts <- bind_rows(equipment_list)

####### EXPORT ####### 
writeToS3 = function(file,bucket,filename){
  s3write_using(file, FUN = write.csv, bucket = bucket, object = filename)
  print(paste('Copy ', nrow(df), ' rows to S3 Bucket ', bucket, ' at ', filename, ' Done!'))
}
writeToS3(mnr_lifts, 'egw-data-dumps', paste('mta/mta-mnr-lifts-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
writeToS3(subway_lifts, 'egw-data-dumps', paste('mta/mta-subway-lifts- , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
