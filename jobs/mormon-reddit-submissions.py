import praw
import pandas as pd
import os

from common import RUN_DATETIME, RUN_LABEL, save_csv

# get environmental variables
PRAW_CLIENT_ID = os.environ['PRAW_CLIENT_ID']
PRAW_CLIENT_SECRET = os.environ['PRAW_CLIENT_SECRET']
PRAW_USER_AGENT = os.environ['PRAW_USER_AGENT']
PRAW_USERNAME = os.environ['PRAW_USERNAME']
PRAW_PASSWORD = os.environ['PRAW_PASSWORD']

# create authorized instance
reddit = praw.Reddit(client_id = PRAW_CLIENT_ID, client_secret = PRAW_CLIENT_SECRET, 
                     user_agent = PRAW_USER_AGENT, username = PRAW_USERNAME, password = PRAW_PASSWORD)                  
 
print(RUN_LABEL)

# get 250 newest submissions for the three "mormon" subreddits
subs = ['mormon', 'exmormon', 'latterdaysaints']
rows = []
for sub in subs:
    for submission in reddit.subreddit(sub).new(limit = 250):
        submission_id = submission.id
        created_utc = submission.created_utc
        title = submission.title
        author = submission.author
        permalink = submission.permalink
        num_comments = submission.num_comments
        score = submission.score
        upvote_ratio = submission.upvote_ratio
        subreddit_name = submission.subreddit.display_name
        rows.append([submission_id, created_utc, title, author, permalink, num_comments, score, upvote_ratio, subreddit_name, RUN_DATETIME])
    
# create dataframe
df = pd.DataFrame(rows)
df.columns = ['submission_id', 'created_utc', 'title', 'author', 'permalink', 'num_comments', 'score', 'upvote_ratio', 'subreddit_name', 'datetime']

# copy to s3
file_path = 'mormon-reddit/mormon-reddit-submissions-' + RUN_LABEL + '.csv'
save_csv(df, file_path)
