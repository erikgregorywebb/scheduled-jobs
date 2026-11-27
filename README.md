# Scheduled Jobs

Python scripts in `jobs/` that collect public data on a schedule and save snapshots to the `egw-data-dumps` S3 bucket, run by GitHub Actions.

- `jobs.yml` lists every job: its script, workflow, schedule, secrets, timeouts and the S3 files it writes. The workflows in `.github/workflows/job_*.yml` and the tables below are generated from it with `python tools/manifest.py`.
- `jobs/common.py` holds the shared helpers: HTTP with retries, S3 writes, one UTC run timestamp. Run any job with `DRY_RUN=1` to write its files under `out/` instead of S3.
- `freshness-check.py` runs daily and fails when a job's newest file is overdue or much smaller than usual.
- `archive/` holds retired scripts, including the R versions of the jobs ported to Python.

<!-- BEGIN GENERATED FROM jobs.yml: edit jobs.yml and run `python tools/manifest.py` -->

## Active

Scheduled by the GitHub Actions workflow in the Workflow column (see `jobs.yml`).

| Name | Frequency | Description | Workflow |
| --- | --- | --- | --- |
| `apple-app-store.py` | Daily | Scrapes top 100 free Finance apps from the [Apple App Store](https://apps.apple.com/us/charts/iphone/finance-apps/6015?chart=top-free), dumps to S3. | `job_1` |
| `apple-app-store-charts.py` | Daily | Collects the top 100 free, paid, and grossing iPhone Finance apps, with rank, from Apple's [iTunes RSS feed](https://itunes.apple.com/us/rss/topfreeapplications/limit=100/genre=6015/json), dumps to S3. | `job_1` |
| `apple-podcast-charts.py` | Daily | Collects the Apple Podcasts top 100 shows overall and in each of 18 categories from the [iTunes RSS feed](https://itunes.apple.com/us/rss/toppodcasts/limit=100/json), plus the top 100 episodes from the [v2 feed](https://rss.marketingtools.apple.com/api/v2/us/podcasts/top/100/podcast-episodes.json), dumps to S3. | `job_1` |
| `freddie-mac-rates.py` | Daily | Scrapes mortgage rates published on the home page of [Freddie Mac](https://www.freddiemac.com/), dumps to S3. | `job_1` |
| `newsapi-top-headlines.py` | Daily | Collects the top headlines from the day from the [News API](https://newsapi.org/), dumps to S3. | `job_1` |
| `spotify-podcast-charts.py` | Daily | Collects the Spotify [podcast charts](https://podcastcharts.byspotify.com/), dumps to S3. | `job_1` |
| `kworb-scrape.py` | Daily | Grabs [kworb](https://kworb.net/) artist, listener, and chart objects, dumps to S3. | `job_1` |
| `last-fm-history.py` | Daily | Collects recent listening history from the [Last.fm API](https://www.last.fm/api), dumps to S3. | `job_1` |
| `nyc-311-requests.py` | Daily | Scrapes the 5,000 latest NYC 311 requests, dumps to S3. | `job_1` |
| `nyc-restaurant-grades.py` | Daily | Snapshots every restaurant in NYC DOHMH's [restaurant inspection results](https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j) (~31,500) with its current and previous letter grade, score, and inspection dates, so grade changes show up between snapshots, dumps to S3. | `job_1` |
| `s3-snapshots.py` | Daily | Saves snapshot of all files and directories within S3 bucket, dumps to S3. | `job_1` |
| `mormon-reddit-submissions.py` | Daily | Grabs the latest 250 posts across the ["mormon"](https://www.reddit.com/r/mormon/) subreddits, dumps to S3. | `job_1` |
| `wikipedia-daily-metrics.py` | Daily | Collects the previous day's most-viewed Wikipedia articles from the [Wikimedia pageviews API](https://wikimedia.org/api/rest_v1/), dumps to S3. | `job_1` |
| `go-fund-me.py` | Daily | Collects stats for the first three featured fundraisers on [GoFundMe](https://www.gofundme.com/discover)'s discover page, dumps to S3. | `job_1` |
| `ny-lottery-winning-numbers.py` | Daily | Collects the latest draws, winning numbers, and NY prize/winner counts for each draw game from the [NY Lottery](https://nylottery.ny.gov/all-winning-numbers), dumps to S3. | `job_1` |
| `treasury-daily.py` | Daily | Collects US Treasury yield curves, auction results and upcoming auctions, Debt to the Penny, and the daily cash balance from [home.treasury.gov](https://home.treasury.gov/resource-center/data-chart-center/interest-rates), [TreasuryDirect](https://www.treasurydirect.gov/), and [Fiscal Data](https://fiscaldata.treasury.gov/), dumps to S3. | `job_1` |
| `tmdb-now-playing.py` | Daily | Collects the movies currently in US theaters from [TMDB](https://developer.themoviedb.org/reference/movie-now-playing-list)'s now playing endpoint, dumps to S3. | `job_1` |
| `lds-meetinghouses.py` | Daily | Collects every [LDS meetinghouse](https://maps.churchofjesuschrist.org/) worldwide (~19,800) with address, coordinates and hours, plus the wards, stakes and other units that meet in each, dumps to S3. | `job_1` |
| `lds-temples.py` | Daily | Collects every [LDS temple](https://www.churchofjesuschrist.org/temples/list) (~385) with its status (operating, renovation, opening soon, construction, announced), plus address, coordinates and hours from the Meetinghouse Locator, milestone dates (announced, groundbreaking, dedicated) from each details page, and upcoming closures, dumps to S3. | `job_1` |
| `faa-aircraft-registry.py` | Daily | Saves the FAA's full [aircraft registry database](https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry/releasable_aircraft_download) zip (every US-registered aircraft, owner, and deregistration) whenever the FAA publishes a new release, dumps to S3. | `job_1` |
| `cnn-fear-greed.py` | Daily | Snapshots the [CNN Fear & Greed Index](https://www.cnn.com/markets/fear-and-greed) from the JSON endpoint behind the page: the headline score and rating (with previous close, 1-week, 1-month and 1-year comparisons), its seven component indicators, and about a year of daily history for each, dumps to S3. | `job_1` |
| `lds-newsroom-rss.py` | Daily | Collects the 25 latest press releases and announcements from the [Church Newsroom](https://newsroom.churchofjesuschrist.org/)'s official [RSS feed](https://newsroom.churchofjesuschrist.org/rss), with title, link, publish date, dateline, summary and image, dumps to S3. | `job_1` |
| `mormon-reddit-comments.py` | Every 4 Hours | Grabs the latest 250 comments per ["mormon"](https://www.reddit.com/r/mormon/) subreddit, dumps to S3. | `job_2` |
| `zillow-scrape.py` | Daily | Scrapes [Zillow](https://www.zillow.com/research/data/)'s basic, zipcode-level ZHVF file, dumps to S3. | `job_3` |
| `tsa-passenger-volumes.py` | Daily | Scrapes daily passenger volume counts reported by the TSA, dumps to S3. | `job_3` |
| `mta-lift-status.py` | Daily | Scrapes elevator and escalator status logs from NY's Metro North Railroad, dumps to S3 under `mta/metro-north-lifts/`. | `job_3` |
| `nyrr-races.py` | Daily | Scrapes a list of upcoming races hosted by NYRR, dumps to S3. | `job_3` |
| `ksl-rentals.py` | Daily | Scrapes [KSL's](https://homes.ksl.com/rent/search) rental unit listings in Utah County, dumps to S3. | `job_3` |
| `hacker-news.py` | Daily | Scrapes the first ten pages of YC's [Hacker News](https://news.ycombinator.com/news?p=1), dumps to S3. | `job_3` |
| `travel-advisories.py` | Daily | Collects the US State Department's [travel advisories](https://travel.state.gov/_res/rss/TAsTWs.xml) from its official RSS feed, dumps to S3. | `job_3` |
| `pinal-county-parcels.py` | Daily | Scrapes parcel details from the Pinal County, Arizona parcel search, dumps to S3. | `job_4` |
| `open-meteo-weather-hourly.py` | Hourly | Collects current weather conditions for Scarsdale, NY from [Open-Meteo](https://open-meteo.com/), dumps to S3. | `job_5` |
| `citibike-station-status.py` | Hourly | Snapshots bikes and docks available at every Citi Bike station from the public [GBFS feed](https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json), with station name, location, and capacity, dumps to S3. | `job_5` |
| `faa-airport-status.py` | Hourly | Snapshots the FAA's live [airport status](https://nasstatus.faa.gov/) feed: ground stops, ground delay programs, arrival/departure delays, airspace flow programs, and closures, dumps to S3. | `job_5` |
| `metro-north-trip-updates.py` | Hourly | Snapshots every Metro-North train in MTA's [GTFS-realtime feed](https://api.mta.info/#/subwayRealTimeFeeds) with its predicted arrival, track, and status at each upcoming stop, joined to the [static schedule](https://rrgtfsfeeds.s3.amazonaws.com/gtfsmnr.zip) for scheduled times and a computed delay, dumps to S3. | `job_5` |
| `metro-north-service-alerts.py` | Hourly | Snapshots Metro-North's [GTFS-realtime service alerts](https://api.mta.info/#/serviceAlerts): live delay and cancellation notices for individual trains (with train number and cause), planned work, schedule changes, and station notices, dumps to S3. | `job_5` |
| `polymarket-markets.py` | Hourly | Snapshots prices, bid/ask, volume and liquidity for the ~4,000 most active open markets on [Polymarket](https://polymarket.com/) (the top 2,000 each by 24-hour volume, total volume and liquidity) from its public [Gamma API](https://docs.polymarket.com/), dumps to S3. | `job_5` |
| `open-meteo-weather-daily.py` | Daily | Backfills the previous day's 24 hourly weather readings for Scarsdale, NY from [Open-Meteo](https://open-meteo.com/), dumps to S3. | `job_6` |
| `freshness-check.py` | Daily | Checks every job's S3 output listed in jobs.yml: fails (so GitHub emails) when a job's newest file is older than its schedule allows, or much smaller than usual, and saves the report, dumps to S3. | `job_7` |

## Inactive

Not scheduled; kept in `archive/` for reference.

| Name | Type | Description |
| --- | --- | --- |
| `archive/spotify-playlist-history.py` | Python | Scrapes track list for Spotify's [Rap Cavier](https://open.spotify.com/playlist/37i9dQZF1DX0XUsuxWHRQd?si=8f0f87a0d4e04e0f) playlist, dumps to S3. |
| `archive/gila-buttes-parcels.r` | R | Scrapes details for ~280 parcels in the Gila Buttes subdivision in Casa Grande, Arizona, dumps to S3. |

<!-- END GENERATED FROM jobs.yml -->
