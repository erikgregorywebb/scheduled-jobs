library(tidyverse)
library(rvest)
library(xml2)
library(aws.s3)

datetime = Sys.time()

# import
urls = sprintf('https://news.ycombinator.com/news?p=%s', seq(1, 10, 1))
datalist = list()
k = 1
for (i in 1:length(urls)) {
  Sys.sleep(5)
  print(urls[i])
  page = read_html(urls[i])
  objects = page %>% html_nodes('.athing')
  subtexts = page %>% html_nodes('.subtext')
  for (j in 1:length(objects)) {
    id = try(objects[j] %>% html_attr('id') %>% as.numeric())
    rank = try(objects[j] %>%html_node('.rank') %>% html_text() %>% as.numeric())
    title = try(objects[j] %>% html_node('.titleline a') %>% html_text())
    link = try(objects[j] %>% html_node('.titleline a') %>% html_attr("href"))
    points = try(subtexts[j] %>% html_nodes('.score') %>% html_text())
    author = try(subtexts[j] %>% html_nodes('.hnuser') %>% html_text())
    time_label = try(subtexts[j] %>% html_nodes('.age a') %>% html_text())
    comment_count = try(subtexts[j] %>% html_nodes('a:nth-last-child(1)') %>% html_text() %>% last())
    temp = tibble(id = id, rank = rank, title = title, link = link, points = points, author = author,
                  time_label = time_label, comment_count = comment_count, source_url = urls[i], scraped_at = datetime)
    datalist[[k]] = temp
    k = k + 1
  }
}
table = do.call(rbind, datalist)

# export
writeToS3 = function(file,bucket,filename){
  s3write_using(file, FUN = write.csv, bucket = bucket, object = filename)
  print(paste('Copy ', nrow(df), ' rows to S3 Bucket ', bucket, ' at ', filename, ' Done!'))
}
writeToS3(table, 'egw-data-dumps', paste('hacker-news/hacker-news-' , format(datetime, '%Y-%m-%d-%H-%M-%S'), '.csv', sep = ''))
