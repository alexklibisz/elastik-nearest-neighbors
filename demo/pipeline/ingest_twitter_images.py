"""
Ingest images from Twitter stream to S3.

Input: listening to the Twitter stream API for statuses containing an image.
Compute: save a local copy of the image and status, upload the image and status
    to an S3 bucket.

Note that this worker uses thread-based concurrency to ingest statuses and images 
    without blocking the Twitter stream. That is, as soon as a Tweet with an 
    image is detected, it gets handed off to a separate thread to download the 
    image, and store the image and status locally and on S3.

"""

from argparse import ArgumentParser
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


class Listener(StreamListener):
    """Extension on the Tweepy StreamListener that implements some simple logic
    for ingesting images from Twitter.

    Arguments
    s3_bucket: a boto3 Bucket instance for bucket where images, statuses are stored.
    images_dir: local directory where images are stored.
    statuses_dir: local directory where statuses are stored.

    """

    def __init__(self, s3_bucket, images_dir, statuses_dir, **kwargs):
        self.cnt_all = len(os.listdir(images_dir))
        self.cnt_new = 0
        self.t0 = time()
        self.s3_bucket = s3_bucket
        self.images_dir = images_dir
        self.statuses_dir = statuses_dir
        super().__init__(kwargs)

    def _ingest_status(self, status):
        """Internal function to ingest a single status

        Can be invoked as a thread to prevent blocking the main loop.

        Arguments
        status: Tweepy Status instance containing at least one image.
        """

        t0 = time()

        # Download first image to disk and upload it to S3.
        # Some statuses have > 1 image, but it"s very rare.
        item = status.entities["media"][0]
        ext = item["media_url"].split(".")[-1]
        image_key = "%d.%s" % (status.id, ext)
        local_path = "%s/%s" % (self.images_dir, image_key)
        urllib.request.urlretrieve(item["media_url"], local_path)
        self.s3_bucket.upload_file(local_path, image_key)

        # Save status to disk as gzipped JSON.
        status_key = "%d.json.gz" % status.id
        local_path = "%s/%s" % (self.statuses_dir, status_key)
        with gzip.open(local_path, "wb") as fp:
            fp.write(json.dumps(status._json).encode())

        self.s3_bucket.upload_file(local_path, status_key)
        print("%.3lf %d" % (time() - t0, status.id))

    def on_status(self, status):
        """Implementation of the function invoked for every new status."""

        # Skip any status not containing images.
        if "media" not in status.entities:
            return

        # Create and start a thread to ingest the status.
        t = Thread(target=self._ingest_status, args=(status,))
        t.start()

        # Book-keeping and logging.
        self.cnt_new += 1
        self.cnt_all += 1
        time_sec = time() - self.t0
        time_day = time_sec / (24 * 60 * 60)
        print("%d total, %d new, %d per day" % (
            self.cnt_all, self.cnt_new, self.cnt_new / time_day))


if __name__ == "__main__":

    ap = ArgumentParser(description="See script header")
    ap.add_argument("--statuses_dir",
                    help="Local directory where statuses are stored",
                    default="data/twitter_stream/statuses")
    ap.add_argument("--images_dir",
                    help="Local directory where images are stored",
                    default="data/twitter_stream/images")
    ap.add_argument("--s3_bucket",
                    default="klibisz-twitter-stream",
                    help="Name of AWS S3 bucket where images and statuses are stored")
    ap.add_argument("--twitter_credentials_path",
                    default="twitter-credentials.json",
                    help="Path to JSON file containing Twitter API credentials")
    args = vars(ap.parse_args())

    # Setup Twitter API client.
    with open(args["twitter_credentials_path"]) as fp:
        twcreds = json.load(fp)
    auth = OAuthHandler(twcreds["consumer_key"], twcreds["consumer_secret"])
    auth.set_access_token(twcreds["access_token"], twcreds["token_secret"])
    twitter = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True,
                  retry_count=10, retry_delay=1)

    # Setup S3 API client using credentials in $HOME/.aws or env. variables.
    s3_bucket = boto3.resource("s3").Bucket(args["s3_bucket"])

    # Setup and run stream listener.
    listener = Listener(s3_bucket, args["images_dir"], args["statuses_dir"])
    stream = Stream(auth=twitter.auth, listener=listener)
    stream.sample()
