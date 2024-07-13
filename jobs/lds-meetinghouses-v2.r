library(tidyverse)
library(dplyr)
library(httr)
library(jsonlite)
library(aws.s3)

datetime = Sys.time()

cookies = c(
  `BIGipServerpool_pvu.cf.churchofjesuschrist.org_HTTP` = "4186455306.20480.0000",
  `TAsessionID` = "4f6c3999-f0e7-48c7-b259-b2854bc3f1a3|NEW",
  `notice_behavior` = "implied|us",
  `at_check` = "true",
  `analytics_video_metadata_load` = "false",
  `mbox` = "session#cf2b50249df94457b6929ccb04bc117b#1720674759|PC#cf2b50249df94457b6929ccb04bc117b.34_0#1783917699"
)

headers = c(
  `Accept` = "application/json",
  `Accept-Language` = "en",
  `Connection` = "keep-alive",
  `DNT` = "1",
  `Referer` = "https://maps.churchofjesuschrist.org/",
  `Sec-Fetch-Dest` = "empty",
  `Sec-Fetch-Mode` = "cors",
  `Sec-Fetch-Site` = "same-origin",
  `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
  `X-Forwarded-Prefix` = "/api/maps-proxy",
  `X-Maps-Client` = "mapsClient",
  `X-Maps-Version` = "2.2.4",
  `X-Trace` = "cc1c8e4ece1946af-34",
  `sec-ch-ua` = '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
  `sec-ch-ua-mobile` = "?0",
  `sec-ch-ua-platform` = '"macOS"'
)

params = list(
  `associated` = "ALL",
  `token` = "dnlt0a"
)

r = httr::GET(url = "https://maps.churchofjesuschrist.org/api/maps-proxy/v2/locations/", httr::add_headers(.headers=headers), query = params, httr::set_cookies(.cookies = cookies))
raw = fromJSON(rawToChar(r$content), flatten = T)

units = raw %>% 
  tibble() %>%
  select(id, type, name, name_display = nameDisplay, type_display = typeDisplay, specialized, provider, notes,
         sub_type = subType, sub_type_display = subTypeDisplay, website, facility_id_identifier = identifiers.facilityId, 
         structure_id = identifiers.structureId, property_id_identifier = identifiers.propertyId, unit_number = identifiers.unitNumber,
         org_id = identifiers.orgId, organization_type_id = organizationType.id, organization_type_code = organizationType.code,
         organization_type_display = organizationType.display, street_1 = address.street1, city = address.city,
         county = address.county, state = address.state, state_id = address.stateId, state_code = address.stateCode,
         postal_code = address.postalCode, country = address.country, country_id = address.countryId, country_code_2 = address.countryCode2,
         country_code_3 = address.countryCode3, formatted_address = address.formatted,
         contact_id = contact.id, contact_type = contact.type, contact_name = contact.name, contact_name_display = contact.nameDisplay,
         contact_contactable = contact.contactable, contact_position_type_id = contact.positionType.id, contact_position_type_code = contact.positionType.code,
         contact_position_type_display = contact.positionType.display, facility_id = facility.id, facility_type = facility.type,
         facility_name_display = facility.nameDisplay, facility_name_display = facility.nameDisplay, facility_type_display = facility.typeDisplay,
         property_id = facility.identifiers.propertyId, parent_id = parent.id, parent_type = parent.type, parent_name_display = parent.nameDisplay,
         parent_type_display = parent.typeDisplay, parent_sub_type = parent.subType, parent_sub_type_display = parent.subTypeDisplay) %>%
mutate(scraped_at = datetime)

associations = raw %>% tibble() %>% select(id, associated) %>% unnest(associated, names_sep = '-') %>%
  select(id, associated_id = `associated-id`, name = `associated-name`, name_display = `associated-nameDisplay`,
         type_display = `associated-typeDisplay`, facility_idd = `associated-identifiers.facilityId`, 
         structure_id = `associated-identifiers.structureId`, property_id = `associated-identifiers.propertyId`,
         unit_number = `associated-identifiers.unitNumber`, org_id = `associated-identifiers.orgId`, sub_type = `associated-subType`,
         sub_type_display = `associated-subTypeDisplay`, language_id = `associated-language.id`, language_code = `associated-language.code`) %>%
mutate(scraped_at = datetime)

writeToS3 = function(file,bucket,filename){
  s3write_using(file, FUN = write.csv, bucket = bucket, object = filename)
}
writeToS3(units, 'egw-data-dumps', paste('lds-meetinghouses/v2/units-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
writeToS3(associations, 'egw-data-dumps', paste('lds-meetinghouses/v2/associations-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
