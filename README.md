## Scheduled Jobs

| Name  | Type | Frequency | Description | Job |
| ------------- | ------------- | ------------- | ------------- | ------------- |
| `apple-app-store.py`  | Python  | Daily  | Scrapes top 100 free Finance apps from the [Apple App Store](https://apps.apple.com/us/charts/iphone/finance-apps/6015?chart=top-free), dumps to S3.  | Job 1 |
| `freddie-mac-rates.py`  | Python  | Daily  | Scrapes mortgage rates published on the home page of [Freddie Mac](https://www.freddiemac.com/), dumps to S3. | Job 1 |
| `mormon-reddit-submissions.py`  | Python  | Daily  | Grabs the latest 250 posts across the ["mormon"](https://www.reddit.com/r/mormon/) subreddits, dumps to S3.  | Job 1 |
| `mormon-reddit-comments.py`  | Python  | Every 4 Hours  | Grabs the latest 250 comments per ["mormon"](https://www.reddit.com/r/mormon/) subreddit, dumps to S3.  | Job 2 |
| `newsapi-top-headlines.py`  | Python  | Daily  | Collects the top headlines from the day from the [News API](https://newsapi.org/), dumps to S3.  | Job 1 |
| `spotify-playlist-history.py`  | Python  | Daily  | Scrapes track list for Spotify's [Rap Cavier](https://open.spotify.com/playlist/37i9dQZF1DX0XUsuxWHRQd?si=8f0f87a0d4e04e0f) playlist, dumps to S3. | Job 1 |
| `spotify-podcast-charts.py`  | Python  | Daily  | Collects the Spotify [podcast charts](https://podcastcharts.byspotify.com/), dumps to S3. | Job 1 |
| `kworb-scrape.py`  | Python  | Daily  | Grabs [kworb](https://kworb.net/) artist, listener, and chart objects, dumps to S3. | Job 1 |
| `gila-buttes-parcels.r`  | R  | Daily  | Scrapes details for ~280 parcels in the Gila Buttes subdivision in Casa Grande, Arizona, dumps to S3. | Job 3 |
| `lds-meetinghouses.r`  | R  | Daily  | Scrapes [LDS meetinghouse](https://ldsmeetinghouses.com/) locations and unit assignments, dumps to S3. | Job 3 |
| `zillow-scrape.r`  | R  | Daily  | Scrapes [Zillow](https://www.zillow.com/research/data/)'s basic, zipcode-level ZHVF file, dumps to S3. | Job 3 |
| `go-fund-me.r`  | R  | Daily  | Scrapes [GoFundMe](https://www.gofundme.com/discover)'s trending fundraiser list, dumps to S3. | Job 3 |
| `s3-snapshots.py`  | Python  | Daily  | Saves snapshot of all files and directories within S3 bucket, dumps to S3.  | Job 1 |
| `tsa-passenger-volumes.r`  | R  | Daily  | Scrapes daily passenger volume counts reported by the TSA, dumps to S3.  | Job 3 |
