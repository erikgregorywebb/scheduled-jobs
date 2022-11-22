## Scheduled Jobs

### Daily

- `apple-app-store.py` scrapes top 100 free Finance apps from the [Apple App Store](https://apps.apple.com/us/charts/iphone/finance-apps/6015?chart=top-free), dumps to S3.
- `freddie-mac-rates.py` scrapes mortgage rates published on the home page of [Freddie Mac](https://www.freddiemac.com/), dumps to S3.
- `spotify-playlist-history.py` scrapes track list for Spotify's [Rap Cavier](https://open.spotify.com/playlist/37i9dQZF1DX0XUsuxWHRQd?si=8f0f87a0d4e04e0f) playlist, dumps to S3.
- `mormon-reddit.py` grabs the latest 250 posts across the ["mormon"](https://www.reddit.com/r/mormon/) subreddits, dumps to S3.
