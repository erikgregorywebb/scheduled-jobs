# Temporary check (branch only): run jobs/go-fund-me.py with the S3 upload stubbed.
import runpy, boto3, pandas as pd
from unittest import mock
class S3:
    def put_object(self, Bucket, Body, Key):
        print('S3 write skipped:', Key)
        from io import StringIO
        df = pd.read_csv(StringIO(Body))
        pd.set_option('display.width', 250); pd.set_option('display.max_columns', 30); pd.set_option('display.max_colwidth', 60)
        print(df.drop(columns=['description']).to_string(index=False))
        print('\ndescription lengths:', df['description'].str.len().tolist())
        print('first description start:', repr(df['description'][0][:300]))
        print('nulls:', df.isna().sum()[lambda s: s > 0].to_dict())
with mock.patch.object(boto3, 'client', lambda *a, **k: S3()):
    runpy.run_path('jobs/go-fund-me.py', run_name='__main__')
