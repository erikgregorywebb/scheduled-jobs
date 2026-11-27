# Temporary check of the rewritten jobs/travel-advisories.r with the S3 upload stubbed (branch only).
s3write_using = function(x, FUN, bucket, object, ...) {
  cat('S3 write skipped:', object, '\n'); cat('rows/cols:', dim(x), '\n')
  print(utils::head(as.data.frame(x), 8)); cat('\nlevels:\n'); print(table(x$level, useNA = 'ifany'))
  cat('\nNAs per column:\n'); print(colSums(is.na(x))); cat('date range:', format(range(x$date_updated, na.rm = TRUE)), '\n')
}
res = tryCatch({ source('jobs/travel-advisories.r'); 'completed' }, error = function(e) paste('ERROR:', conditionMessage(e)))
cat('script result:', res, '\n')
