"""Populate Elasticsearch-aknn documents from feature docs on disk or S3.
"""

from argparse import ArgumentParser
from io import BytesIO
from numpy import mean, std
from pprint import pformat
from time import time
import boto3
import json
import gzip
import os
import pdb
import random
import requests
import sys


def iter_feature_docs(src):

    if src.startswith("s3://"):
        bucket = boto3.resource("s3").Bucket(src.replace("s3://", ''))
        for obj in bucket.objects.all():
            body = obj.get().get('Body')
            buff = BytesIO(body.read())
            with gzip.open(buff) as fp:
                yield json.loads(fp.read().decode())
    else:
        for fobj in os.scandir(src):
            with gzip.open(fobj.path) as fp:
                yield json.loads(fp.read().decode())


if __name__ == "__main__":

    ap = ArgumentParser(description="See script")
    ap.add_argument("features_src",
                    help="Directory or S3 bucket containing image feature docs.")
    ap.add_argument("--es_host", default="http://localhost:9200",
                    help="URL of single elasticsearch server.")
    ap.add_argument("-b", "--batch_size", type=int, default=1000,
                    help="Batch size for elasticsearch indexing.")
    args = vars(ap.parse_args())

    # Prepare the document structure.
    payload = {
        "_index": "twitter_images",
        "_type": "twitter_image",
        "_aknn_uri": "aknn_models/aknn_model/twitter_images",
        "_aknn_docs": [
            # Populated below with structure:
            # {
            #     "_id": "...",
            #     "_source": {
            #         "any_fields_you_want": "...",
            #         "_aknn_vector": [0.1, 0.2, ...]
            #     }
            # }, ...
        ]
    }

    # Create an iterable over the feature documents.
    feature_docs = iter_feature_docs(args["features_src"])

    for fd in feature_docs:
        payload["_aknn_docs"].append({
            "_id": fd["id"],
            "_source": {
                "twitter_url": "https://twitter.com/statuses/%s" % fd["id"],
                "imagenet_labels": fd["imagenet_labels"],
                "_aknn_vector": fd["feature_vector"]
            }
        })

        if len(payload["_aknn_docs"]) < args["batch_size"]:
            continue

        print("Posting %d new docs to Elasticsearch" % (
            len(payload["_aknn_docs"])))

        t0 = time()
        res = requests.post("%s/_aknn_index" % args["es_host"], json=payload)
        if res.status_code == requests.codes.ok:
            print("Successfully indexed in %d seconds" % (time() - t0))
            print(pformat(res.json()))
        else:
            print("Failed with error code %d" % res.status_code, file=sys.stderr)
            sys.exit(1)

        payload["_aknn_docs"] = []
