import praw
import pandas as pd
import os

from common import RUN_DATETIME, RUN_LABEL, save_csv

# get environmental variables
PRAW_CLIENT_ID = os.environ["PRAW_CLIENT_ID"]
PRAW_CLIENT_SECRET = os.environ["PRAW_CLIENT_SECRET"]
PRAW_USER_AGENT = os.environ["PRAW_USER_AGENT"]
PRAW_USERNAME = os.environ["PRAW_USERNAME"]
PRAW_PASSWORD = os.environ["PRAW_PASSWORD"]

# create authorized instance
reddit = praw.Reddit(
    client_id=PRAW_CLIENT_ID,
    client_secret=PRAW_CLIENT_SECRET,
    user_agent=PRAW_USER_AGENT,
    username=PRAW_USERNAME,
    password=PRAW_PASSWORD,
)

# get comments
# https://praw.readthedocs.io/en/stable/code_overview/models/comment.html
rows = []
subs = ["mormon", "exmormon", "latterdaysaints"]
for sub in subs:
    for comment in reddit.subreddit(sub).comments(limit=250):
        try:
            comment_id = comment.id
            created_utc = comment.created_utc
            body = comment.body
            author = comment.author.name
            permalink = comment.permalink
            link_id = comment.link_id
            score = comment.score
            submission_id = comment.submission.id
            subreddit_name = comment.subreddit.display_name
            rows.append(
                [
                    comment_id,
                    created_utc,
                    body,
                    author,
                    permalink,
                    link_id,
                    score,
                    submission_id,
                    subreddit_name,
                    RUN_DATETIME,
                ]
            )
        except:
            comment_id = comment.id
            created_utc = None
            body = None
            author = None
            permalink = None
            link_id = None
            score = None
            submission_id = None
            subreddit_name = None
            rows.append(
                [
                    comment_id,
                    created_utc,
                    body,
                    author,
                    permalink,
                    link_id,
                    score,
                    submission_id,
                    subreddit_name,
                    RUN_DATETIME,
                ]
            )

# create dataframe
df = pd.DataFrame(rows)
df.columns = [
    "comment_id",
    "created_utc",
    "body",
    "author",
    "permalink",
    "link_id",
    "score",
    "submission_id",
    "subreddit_name",
    "datetime",
]

# copy to s3
file_path = "mormon-reddit/mormon-reddit-comments-" + RUN_LABEL + ".csv"
save_csv(df, file_path)
