# Temporary diagnostic for jobs/travel-advisories.r (branch only, not for main).
suppressPackageStartupMessages({library(rvest); library(dplyr)})
url = 'https://travel.state.gov/content/travel/en/traveladvisories/traveladvisories.html/'
page = tryCatch(read_html(url), error = function(e) {cat('read_html ERROR:', conditionMessage(e), '\n'); NULL})
if (!is.null(page)) {
  tables = html_table(page)
  cat('tables found:', length(tables), '\n')
  if (length(tables) > 0) { cat('first table columns:', paste(names(tables[[1]]), collapse = ' | '), '\n'); print(head(tables[[1]], 5)) }
}
cat('\n===== Original script, S3 upload stubbed =====\n')
s3write_using = function(x, FUN, bucket, object, ...) {
  cat('S3 write skipped:', object, '\n'); print(dim(x)); print(utils::head(as.data.frame(x), 5))
}
res = tryCatch({ source('jobs/travel-advisories.r'); 'completed' }, error = function(e) paste('ERROR:', conditionMessage(e)))
cat('original script result:', res, '\n')
