import boto3
import pandas as pd
from datetime import datetime
from io import StringIO
import os

# get current date and datetime
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_datetime_label = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

# get bucket creds
bucket_name = 'egw-data-dumps'
path = ''
access_key = os.environ['AWS_ACCESS_KEY_ID']
secret_key = os.environ['AWS_SECRET_ACCESS_KEY']

# define function
def list_files_in_bucket(bucket_name, path, access_key, secret_key):
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    files = []
    
    # Initial call to list objects with a specified path (prefix)
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=path)
    
    while response.get('Contents'):
        for obj in response['Contents']:
            if obj['Key'] != path:  # Exclude the root directory or specific path itself
                files.append({
                    'key': obj['Key'],
                    'last_modified': obj['LastModified'],
                    'size': obj['Size'],
                    'e_tag': obj['ETag']
                })

        # If the response is truncated, there's more data to fetch
        if response['IsTruncated']:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=path, ContinuationToken=response['NextContinuationToken'])
        else:
            break

    df = pd.DataFrame(files)
    return df

# run
df = list_files_in_bucket(bucket_name, path, access_key, secret_key)

# copy to s3
s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')

file_path = 'data-pipeline-logs/s3-snapshot-' + current_datetime_label + '.csv'
copy_to_s3(client=s3, df=df, bucket='egw-data-dumps', filepath=file_path)
