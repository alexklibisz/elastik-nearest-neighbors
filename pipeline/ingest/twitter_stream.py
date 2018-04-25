"""Listen to the Twitter stream API for statuses containing an image.
For each status with an image, ingest it by saving a local copy of the
status and image, uploading the status and image to S3, and adding an 
entry to Amazon DnyamoDB."""

from io import BytesIO
from tweepy import OAuthHandler, API, Stream, StreamListener
from threading import Thread
from time import time
import boto3
import json
import gzip
import urllib.request
import os
import pdb
import sys

STATUSES_DIR = "data/twitter_stream/statuses_online"
IMAGES_DIR = "data/twitter_stream/images_online"
DYNAMO_TABLE = "klibisz-twitter-stream"
S3_BUCKET = "klibisz-twitter-stream"
TWCREDS = {
    "consumer_key": os.getenv("TWCK"),
    "consumer_secret": os.getenv("TWCS"),
    "access_token": os.getenv("TWAT"),
    "token_secret": os.getenv("TWTS")
}


def ingest(status, s3_client, dynamo_table):

    t0 = time()

    # Download first image to disk and upload it to S3.
    # Some statuses have > 1 image, but it's very rare.
    item = status.entities['media'][0]
    ext = item['media_url'].split('.')[-1]
    image_key = '%d.%s' % (status.id, ext)
    local_path = '%s/%s' % (IMAGES_DIR, image_key)
    urllib.request.urlretrieve(item['media_url'], local_path)
    s3_client.upload_file(local_path, Bucket=S3_BUCKET, Key=image_key)

    # Save status to disk.
    status_key = '%d.json.gz' % status.id
    local_path = '%s/%s' % (STATUSES_DIR, status_key)
    with gzip.open(local_path, 'wb') as fp:
        fp.write(json.dumps(status._json).encode())

    # Upload to S3 without reading from disk.
    # status_body = json.dumps(status._json)
    # status_body = status_body.encode()
    # status_body = gzip.compress(status_body)
    # status_body = BytesIO(status_body)
    # s3_client.put_object(Body=status_body, Bucket=S3_BUCKET, Key=status_key)
    s3_client.upload_file(local_path, Bucket=S3_BUCKET, Key=status_key)

    # # Write item to DynamoDB.
    # item = dict(status_id=status.id,
    #             image_bucket=S3_BUCKET, image_key=image_key,
    #             status_bucket=S3_BUCKET, status_key=status_key)
    # dynamo_table.put_item(Item=item)

    print('%.3lf %d' % (time() - t0, status.id))


class ImageTweetStreamListener(StreamListener):

    def __init__(self, s3_client, dynamo_table, **kwargs):
        self.cnt_all = len(os.listdir(IMAGES_DIR))
        self.cnt_new = 0
        self.t0 = time()
        self.s3_client = s3_client
        self.dynamo_table = dynamo_table
        super().__init__(kwargs)

    def on_status(self, status):

        if 'media' not in status.entities:
            return

        args = (status, self.s3_client, self.dynamo_table)
        thr = Thread(target=ingest, args=args)
        thr.start()

        self.cnt_new += 1
        self.cnt_all += len(status.entities['media'])

        time_sec = time() - self.t0
        time_day = time_sec / (24 * 60 * 60)

        print('%d total, %d new, %.3lf per day' % (
            self.cnt_all, self.cnt_new, self.cnt_new / time_day))


if __name__ == "__main__":

    auth = OAuthHandler(TWCREDS['consumer_key'], TWCREDS['consumer_secret'])
    auth.set_access_token(TWCREDS['access_token'], TWCREDS['token_secret'])
    twitter = API(
        auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True,
        retry_count=10, retry_delay=1)

    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('aws_access_key_id'),
        aws_secret_access_key=os.getenv('aws_secret_access_key'))

    dynamo_handle = boto3.resource(
        'dynamodb',
        region_name="us-east-1",
        aws_access_key_id=os.getenv('aws_access_key_id'),
        aws_secret_access_key=os.getenv('aws_secret_access_key')
    )
    dynamo_table = dynamo_handle.Table(DYNAMO_TABLE)

    listener = ImageTweetStreamListener(s3_client, dynamo_table)
    stream = Stream(auth=twitter.auth, listener=listener)
    stream.sample()
