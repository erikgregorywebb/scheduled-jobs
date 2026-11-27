# Scheduled Jobs

## Active

Scheduled to run by the GitHub Actions workflow named in the Job column.

| Name  | Type | Frequency | Description | Job |
| ------------- | ------------- | ------------- | ------------- | ------------- |
| `apple-app-store.py`  | Python  | Daily  | Scrapes top 100 free Finance apps from the [Apple App Store](https://apps.apple.com/us/charts/iphone/finance-apps/6015?chart=top-free), dumps to S3.  | Job 1 |
| `apple-app-store-charts.py`  | Python  | Daily  | Collects the top 100 free, paid, and grossing iPhone Finance apps, with rank, from Apple's [iTunes RSS feed](https://itunes.apple.com/us/rss/topfreeapplications/limit=100/genre=6015/json), dumps to S3. | Job 1 |
| `apple-podcast-charts.py`  | Python  | Daily  | Collects the Apple Podcasts top 100 shows overall and in each of 18 categories from the [iTunes RSS feed](https://itunes.apple.com/us/rss/toppodcasts/limit=100/json), plus the top 100 episodes from the [v2 feed](https://rss.marketingtools.apple.com/api/v2/us/podcasts/top/100/podcast-episodes.json), dumps to S3. | Job 1 |
| `freddie-mac-rates.py`  | Python  | Daily  | Scrapes mortgage rates published on the home page of [Freddie Mac](https://www.freddiemac.com/), dumps to S3. | Job 1 |
| `mormon-reddit-submissions.py`  | Python  | Daily  | Grabs the latest 250 posts across the ["mormon"](https://www.reddit.com/r/mormon/) subreddits, dumps to S3.  | Job 1 |
| `mormon-reddit-comments.py`  | Python  | Every 4 Hours  | Grabs the latest 250 comments per ["mormon"](https://www.reddit.com/r/mormon/) subreddit, dumps to S3.  | Job 2 |
| `newsapi-top-headlines.py`  | Python  | Daily  | Collects the top headlines from the day from the [News API](https://newsapi.org/), dumps to S3.  | Job 1 |
| `spotify-podcast-charts.py`  | Python  | Daily  | Collects the Spotify [podcast charts](https://podcastcharts.byspotify.com/), dumps to S3. | Job 1 |
| `kworb-scrape.py`  | Python  | Daily  | Grabs [kworb](https://kworb.net/) artist, listener, and chart objects, dumps to S3. | Job 1 |
| `zillow-scrape.r`  | R  | Daily  | Scrapes [Zillow](https://www.zillow.com/research/data/)'s basic, zipcode-level ZHVF file, dumps to S3. | Job 3 |
| `s3-snapshots.py`  | Python  | Daily  | Saves snapshot of all files and directories within S3 bucket, dumps to S3.  | Job 1 |
| `tsa-passenger-volumes.r`  | R  | Daily  | Scrapes daily passenger volume counts reported by the TSA, dumps to S3.  | Job 3 |
| `mta-lift-status.r`  | R  | Daily  | Scrapes elevator and escalator status logs from NY's Metro North Railroad, dumps to S3.  | Job 3 |
| `nyc-311-requests.py`  | Python  | Daily  | Scrapes 10k of the latest NYC 311 requests, dumps to S3. | Job 1 |
| `nyrr-races.r`  | R  | Daily  | Scrapes a list of upcoming races hosted by NYRR, dumps to S3. | Job 3 |
| `ksl-rentals.r`  | R  | Daily  | Scrapes [KSL's](https://homes.ksl.com/rent/search) rental unit listings in Utah County, dumps to S3. | Job 3 |
| `hacker-news.r`  | R  | Daily  | Scrapes the first ten pages of YC's [Hacker News](https://news.ycombinator.com/news?p=1), dumps to S3. | Job 3 |
| `open-meteo-weather-hourly.py`  | Python  | Hourly  | Collects current weather conditions for Scarsdale, NY from [Open-Meteo](https://open-meteo.com/), dumps to S3. | Job 5 |
| `citibike-station-status.py`  | Python  | Hourly  | Snapshots bikes and docks available at every Citi Bike station from the public [GBFS feed](https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json), with station name, location, and capacity, dumps to S3. | Job 5 |
| `open-meteo-weather-daily.py`  | Python  | Daily  | Backfills the previous day's 24 hourly weather readings for Scarsdale, NY from [Open-Meteo](https://open-meteo.com/), dumps to S3. | Job 6 |
| `last-fm-history.py`  | Python  | Daily  | Collects recent listening history from the [Last.fm API](https://www.last.fm/api), dumps to S3. | Job 1 |
| `wikipedia-daily-metrics.py`  | Python  | Daily  | Collects the previous day's most-viewed Wikipedia articles from the [Wikimedia pageviews API](https://wikimedia.org/api/rest_v1/), dumps to S3. | Job 1 |
| `lds-meetinghouses.py`  | Python  | Daily  | Collects every [LDS meetinghouse](https://maps.churchofjesuschrist.org/) worldwide (~19,800) with address, coordinates and hours, plus the wards, stakes and other units that meet in each, dumps to S3. | Job 1 |
| `lds-temples.py`  | Python  | Daily  | Collects every [LDS temple](https://www.churchofjesuschrist.org/temples/list) (~385) with its status (operating, renovation, opening soon, construction, announced), plus address, coordinates and hours from the Meetinghouse Locator, milestone dates (announced, groundbreaking, dedicated) from each details page, and upcoming closures, dumps to S3. | Job 1 |
| `pinal-county-parcels.py`  | Python  | Daily  | Scrapes parcel details from the Pinal County, Arizona parcel search, dumps to S3. | Job 4 |
| `go-fund-me.py`  | Python  | Daily  | Collects stats for the first three featured fundraisers on [GoFundMe](https://www.gofundme.com/discover)'s discover page, dumps to S3. | Job 1 |
| `travel-advisories.r`  | R  | Daily  | Collects the US State Department's [travel advisories](https://travel.state.gov/_res/rss/TAsTWs.xml) from its official RSS feed, dumps to S3. | Job 3 |
| `ny-lottery-winning-numbers.py`  | Python  | Daily  | Collects the latest draws, winning numbers, and NY prize/winner counts for each draw game from the [NY Lottery](https://nylottery.ny.gov/all-winning-numbers), dumps to S3. | Job 1 |
| `treasury-daily.py`  | Python  | Daily  | Collects US Treasury yield curves, auction results and upcoming auctions, Debt to the Penny, and the daily cash balance from [home.treasury.gov](https://home.treasury.gov/resource-center/data-chart-center/interest-rates), [TreasuryDirect](https://www.treasurydirect.gov/), and [Fiscal Data](https://fiscaldata.treasury.gov/), dumps to S3. | Job 1 |
| `tmdb-now-playing.py`  | Python  | Daily  | Collects the movies currently in US theaters from [TMDB](https://developer.themoviedb.org/reference/movie-now-playing-list)'s now playing endpoint, dumps to S3. | Job 1 |

## Inactive

Not scheduled in any workflow.

| Name  | Type | Description |
| ------------- | ------------- | ------------- |
| `spotify-playlist-history.py`  | Python  | Scrapes track list for Spotify's [Rap Cavier](https://open.spotify.com/playlist/37i9dQZF1DX0XUsuxWHRQd?si=8f0f87a0d4e04e0f) playlist, dumps to S3. |
| `gila-buttes-parcels.r`  | R  | Scrapes details for ~280 parcels in the Gila Buttes subdivision in Casa Grande, Arizona, dumps to S3. |
