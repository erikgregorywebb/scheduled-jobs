# Temporary check (branch only): run jobs/travel-advisories.r with the S3 upload stubbed.
s3write_using = function(x, FUN, bucket, object, ...) {
  cat('S3 write skipped:', object, ' rows/cols:', dim(x), '\n')
  cat('NAs:', sum(is.na(x)), '\n')
  cat('Mexico rows:\n'); print(as.data.frame(x[grepl('Mexico', x$advisory), c('advisory', 'level', 'date_updated')]))
  cat('names containing "Travel Advisory":', sum(grepl('Travel Advisory', x$advisory)), '\n')
  mapped = c('Burma (Myanmar)', "Côte d'Ivoire (Ivory-Coast)", 'Nauru', 'Bahamas', 'United Kingdom of Great Britain and Northern Ireland', 'Saint Barthélemy (French West Indies)')
  cat('mapped names present:\n'); print(setNames(mapped %in% x$advisory, mapped))
}
res = tryCatch({ source('jobs/travel-advisories.r'); 'completed' }, error = function(e) paste('ERROR:', conditionMessage(e)))
cat('script result:', res, '\n')
