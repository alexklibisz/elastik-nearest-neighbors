"""Reconcile statuses and images that were ingested without storing to S3/Dynamo"""
from glob import glob
from time import time
import boto3
import json
import gzip
import os
import pdb
import sys

STATUSES_DIR = "data/twitter_stream/statuses_local_only"
IMAGES_DIR = "data/twitter_stream/images_local_only"

STATUSES_DIR_RECONCILED = "data/twitter_stream/statuses_online"
IMAGES_DIR_RECONCILED = "data/twitter_stream/images_online"

DYNAMO_TABLE = "klibisz-twitter-stream"
S3_BUCKET = "klibisz-twitter-stream"


def reconcile(status_path, s3_client, dynamo_table):

    # Read status from disk.
    with gzip.open(status_path, 'rb') as fp:
        status = json.loads(fp.read().decode())

    # Upload image to S3.
    item = status['entities']['media'][0]
    ext = item['media_url'].split('.')[-1]
    image_key = '%d.%s' % (status['id'], ext)
    image_path = '%s/%s' % (IMAGES_DIR, image_key)

    # Try both image names. Originally images had a _0 appended. If that's the
    # case, go ahead and rename the image to its proper name.
    try:
        s3_client.upload_file(image_path, Bucket=S3_BUCKET, Key=image_key)
    except FileNotFoundError:
        image_path_old = image_path.replace('.', '_0.')
        os.rename(image_path_old, image_path)
        s3_client.upload_file(image_path, Bucket=S3_BUCKET, Key=image_key)
    except Exception as ex:
        print(status_path, ex)
        return

    # Upload status to S3.
    status_key = '%d.json.gz' % status['id']
    s3_client.upload_file(status_path, Bucket=S3_BUCKET, Key=status_key)

    # Write item to DynamoDB.
    item = dict(status_id=status['id'],
                image_bucket=S3_BUCKET, image_key=image_key,
                status_bucket=S3_BUCKET, status_key=status_key)
    dynamo_table.put_item(Item=item)

    # Copy local files into reconciled directory.
    os.rename(status_path, status_path.replace(STATUSES_DIR, STATUSES_DIR_RECONCILED))
    os.rename(image_path, image_path.replace(IMAGES_DIR, IMAGES_DIR_RECONCILED))


if __name__ == "__main__":

    dynamo_handle = boto3.resource(
        'dynamodb',
        region_name="us-east-1",
        aws_access_key_id=os.getenv('aws_access_key_id'),
        aws_secret_access_key=os.getenv('aws_secret_access_key')
    )

    dynamo_table = dynamo_handle.Table(DYNAMO_TABLE)

    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('aws_access_key_id'),
        aws_secret_access_key=os.getenv('aws_secret_access_key'))

    status_paths = glob('%s/*.json.gz' % STATUSES_DIR)

    for i, status_path in enumerate(status_paths):
        reconcile(status_path, s3_client, dynamo_table)
        print('%d of %d' % (i + 1, len(status_paths)))
