import praw
from datetime import datetime
import pandas as pd
import boto3
from io import StringIO
import os

# get environmental variables
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
PRAW_CLIENT_ID = os.environ['PRAW_CLIENT_ID']
PRAW_CLIENT_SECRET = os.environ['PRAW_CLIENT_SECRET']
PRAW_USER_AGENT = os.environ['PRAW_USER_AGENT']
PRAW_USERNAME = os.environ['PRAW_USERNAME']
PRAW_PASSWORD = os.environ['PRAW_PASSWORD']

# create authorized instance
reddit = praw.Reddit(client_id = PRAW_CLIENT_ID, client_secret = PRAW_CLIENT_SECRET, 
                     user_agent = PRAW_USER_AGENT, username = PRAW_USERNAME, password = PRAW_PASSWORD)
print('that worked')                    
 
# get current date and datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
print(current_datetime_label)

# get 250 newest submissions for the three "mormon" subreddits
rows = []
for submission in reddit.subreddit('mormon+exmormon+latterdaysaints').new(limit = 250):
    # capture comment data
    submission_id = submission.id
    created_utc = submission.created_utc
    title = submission.title
    print(title)
    author = submission.author
    permalink = submission.permalink
    num_comments = submission.num_comments
    score = submission.score
    upvote_ratio = submission.upvote_ratio
    subreddit_name = submission.subreddit.display_name
    rows.append([submission_id, created_utc, title, author, permalink, num_comments, score, upvote_ratio, subreddit_name, current_datetime])
    
# create dataframe
df = pd.DataFrame(rows)
df.columns = ['submission_id', 'created_utc', 'title', 'author', 'permalink', 'num_comments', 'score', 'upvote_ratio', 'subreddit_name', 'datetime']

# copy to s3
# https://stackoverflow.com/questions/38154040/save-dataframe-to-csv-directly-to-s3-python
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

file_path = 'mormon-reddit/mormon-reddit-submissions-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath=file_path)
