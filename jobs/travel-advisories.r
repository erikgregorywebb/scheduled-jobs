library(tidyverse)
library(lubridate)
library(httr)
library(xml2)
library(aws.s3)

datetime = Sys.time()

# import: the State Department's official travel advisory RSS feed
# (the advisories HTML page is behind a Cloudflare check that blocks scripts)
url = 'https://travel.state.gov/_res/rss/TAsTWs.xml'
res = GET(url, user_agent('Mozilla/5.0 (compatible; scheduled-jobs/1.0)'), timeout(60))
stop_for_status(res)
feed = read_xml(content(res, as = 'text', encoding = 'UTF-8'))
items = xml_find_all(feed, '//item')
if (length(items) == 0) stop('Travel advisory feed returned no items')

# feed names mapped back to the names used in files saved from the old HTML table,
# so destinations line up across the history. Not mapped: Israel, Gaza and The West
# Bank (one combined entry before) and Saba and Sint Eustatius (listed differently before).
name_map = c(
  'Burma' = 'Burma (Myanmar)',
  'Côte d’Ivoire' = "Côte d'Ivoire (Ivory-Coast)",
  'Democratic Republic of the Congo' = 'Democratic Republic of the Congo (D.R.C.)',
  'Eswatini' = 'Eswatini (Swaziland)',
  'French Saint Martin' = 'Saint Martin (French West Indies)',
  'Guadeloupe' = 'Guadeloupe (French West Indies)',
  'Martinique' = 'Martinique (French West Indies)',
  'Naoero' = 'Nauru',
  'North Korea' = "North Korea (Democratic People's Republic of Korea)",
  'North Macedonia' = 'Republic of North Macedonia',
  'Republic of Congo' = 'Republic of the Congo',
  'Saint Barthelemy' = 'Saint Barthélemy (French West Indies)',
  'São Tomé and Príncipe' = 'Sao Tome and Principe',
  'The Bahamas' = 'Bahamas',
  'United Kingdom' = 'United Kingdom of Great Britain and Northern Ireland'
)

# clean: titles look like "Afghanistan - Level 4: Do Not Travel"
table = tibble(
    title = xml_text(xml_find_first(items, 'title')),
    url = xml_text(xml_find_first(items, 'link')),
    pub_date = xml_text(xml_find_first(items, 'pubDate'))
  ) %>%
  mutate(
    advisory = str_trim(str_remove(str_remove(title, '\\s+-\\s+Level\\s+\\d.*$'), '\\s+Travel Advisory$')),
    advisory = coalesce(unname(name_map[advisory]), advisory),
    # sentence case, e.g. "Level 4: Do not travel", to match files saved from the old HTML table
    level = paste0(str_extract(title, 'Level\\s+\\d'), ': ', str_to_sentence(str_trim(str_extract(title, '(?<=Level\\s\\d:).*$')))),
    date_updated = as_date(dmy(str_remove(pub_date, '^[A-Za-z]+,\\s*'))),
    scraped_at = datetime
  ) %>%
  select(advisory, level, date_updated, url, scraped_at)

####### EXPORT #######
writeToS3 = function(file,bucket,filename){
  s3write_using(file, FUN = write.csv, bucket = bucket, object = filename)
  print(paste('Copy ', nrow(file), ' rows to S3 Bucket ', bucket, ' at ', filename, ' Done!'))
}
writeToS3(table, 'egw-data-dumps', paste('travel-advisories/travel-advisories-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
