# Temporary diagnostic for jobs/go-fund-me.r (branch only, not for main).
suppressPackageStartupMessages({library(rvest); library(httr); library(dplyr)})

cat('\n===== 1. Discover page =====\n')
r = GET('https://www.gofundme.com/discover')
cat('status:', status_code(r), ' bytes:', length(content(r, 'raw')), ' final url:', r$url, '\n')
html = content(r, 'text', encoding = 'UTF-8')
page = read_html(html)
cat('<title>:', page %>% html_node('title') %>% html_text2(), '\n')
cat('looks like a bot challenge:', grepl('cf-chl|challenge-platform|Just a moment|captcha', html, ignore.case = TRUE), '\n')
n_old = length(html_nodes(page, '#campaign_click_on_discover_page'))
cat('old selector #campaign_click_on_discover_page matches:', n_old, '\n')
hrefs = page %>% html_nodes('a') %>% html_attr('href')
f_links = unique(grep('/f/', hrefs, value = TRUE))
cat('links containing /f/:', length(f_links), '\n'); print(head(f_links, 10))
ids = unique(regmatches(html, gregexpr('id="[^"]*campaign[^"]*"', html))[[1]])
cat('element ids mentioning campaign:\n'); print(head(ids, 20))
testids = unique(regmatches(html, gregexpr('data-testid="[^"]*"', html))[[1]])
cat('data-testid values (first 40):\n'); print(head(testids, 40))

cat('\n===== 2. First campaign page =====\n')
if (length(f_links) > 0) {
  u = f_links[1]; if (startsWith(u, '/')) u = paste0('https://www.gofundme.com', u)
  r2 = GET(u); cat('url:', u, ' status:', status_code(r2), '\n')
  h2 = content(r2, 'text', encoding = 'UTF-8'); p2 = read_html(h2)
  for (sel in c('.p-campaign-title', '.progress-meter_progressMeterHeading__7dug0', '.progress-meter_progressMeter__ebbGu',
                '.a-created-date', '.o-campaign-description', '.m-campaign-byline-description', 'h1')) {
    m = html_nodes(p2, sel)
    cat(sprintf('%-50s matches: %d  first: %s\n', sel, length(m), substr(paste(html_text2(m[1]), collapse=''), 1, 80)))
  }
  classes = unique(regmatches(h2, gregexpr('progress-meter_[A-Za-z0-9_-]+', h2))[[1]])
  cat('progress-meter classes present now:\n'); print(classes)
  cat('has __NEXT_DATA__ JSON:', grepl('__NEXT_DATA__', h2), '\n')
}

cat('\n===== 3. Original script, S3 upload stubbed =====\n')
s3write_using = function(x, FUN, bucket, object, ...) {
  cat('S3 write skipped:', object, '\n'); print(dim(x)); print(utils::head(as.data.frame(x), 5))
}
res = tryCatch({ source('jobs/go-fund-me.r'); 'completed' }, error = function(e) paste('ERROR:', conditionMessage(e)))
cat('original script result:', res, '\n')
