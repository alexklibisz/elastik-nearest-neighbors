"""Populate Elasticsearch-aknn documents from feature docs on disk or S3.
"""

from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from itertools import cycle
from more_itertools import chunked
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


def iter_docs(src, skip=0, alphasort=False):

    if src.startswith("s3://"):
        bucket = boto3.resource("s3").Bucket(src.replace("s3://", ''))
        for i, obj in enumerate(bucket.objects.all()):
            if i < skip:
                continue
            body = obj.get().get('Body')
            buff = BytesIO(body.read())
            with gzip.open(buff) as fp:
                yield json.loads(fp.read().decode())
    else:

        if alphasort:
            iter_ = sorted(os.scandir(src), key=lambda f: f.path)
        else:
            iter_ = os.scandir(src)

        for i, fobj in enumerate(iter_):
            if i < skip:
                continue
            with gzip.open(fobj.path) as fp:
                yield json.loads(fp.read().decode())


if __name__ == "__main__":

    ap = ArgumentParser(description="See script")
    ap.add_argument("features_src",
                    help="Directory or S3 bucket containing image feature docs.")
    ap.add_argument("--es_hosts", default="http://localhost:9200",
                    help="Comma-separated elasticsearch host URLs.")
    ap.add_argument("-b", "--batch_size", type=int, default=1000,
                    help="Batch size for elasticsearch indexing.")
    args = vars(ap.parse_args())

    # Parse multiple hosts.
    es_hosts = args["es_hosts"].split(",")
    es_hosts_cycle = cycle(es_hosts)

    # Prepare the document structure.
    body = {
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

    mapping = {
        "properties": {
            "_aknn_vector": {
                "type": "half_float",
                "index": False
            }
        }
    }

    # Check if the index exists and get its count.
    count_url = "%s/%s/%s/_count" % (next(es_hosts_cycle), body["_index"], body["_type"])
    req = requests.get(count_url)
    count = 0 if req.status_code == 404 else req.json()["count"]
    print("Found %d existing documents in index" % count)

    # If the index does not exist, create its mapping.
    if req.status_code == 404:
        print("Creating index %s" % body["_index"])
        index_url = "%s/%s" % (next(es_hosts_cycle), body["_index"])
        req = requests.put(index_url)
        assert req.status_code == 200, json.dumps(req.json())

        print("Creating mapping for type %s" % body["_type"])
        mapping_url = "%s/%s/%s/_mapping" % (
            next(es_hosts_cycle), body["_index"], body["_type"])
        requests.put(mapping_url, json=mapping)
        assert req.status_code == 200, json.dumps(req.json())

    # Create an iterable over the feature documents.
    docs = iter_docs(args["features_src"], count, True)

    # Bookkeeping for round-robin indexing.
    docs_batch = []
    tpool = ThreadPoolExecutor(max_workers=len(es_hosts))
    nb_round_robin_rem = len(es_hosts) * args["batch_size"]
    nb_indexed = 0
    T0 = -1

    for doc in docs:

        if T0 < 0:
            T0 = time()

        aknn_doc = {
            "_id": doc["id"],
            "_source": {
                "twitter_url": "https://twitter.com/statuses/%s" % doc["id"],
                "imagenet_labels": doc["imagenet_labels"],
                "s3_url": "https://s3.amazonaws.com/%s/%s" % (
                    doc["img_pointer"]["s3_bucket"], doc["img_pointer"]["s3_key"]),
                "_aknn_vector": doc["feature_vector"]
            }
        }

        docs_batch.append(aknn_doc)
        nb_round_robin_rem -= 1
        if nb_round_robin_rem > 0:
            continue

        futures = []
        for h, d in zip(es_hosts, chunked(docs_batch, args["batch_size"])):
            body["_aknn_docs"] = d
            post_url = "%s/_aknn_index" % h
            futures.append(tpool.submit(requests.post, post_url, json=body))
            print("Posting %d docs to host %s" % (len(body["_aknn_docs"]), h))

        for f, h in zip(as_completed(futures), es_hosts):
            res = f.result()
            if res.status_code != 200:
                print("Error at host: %s" % h, res.json(), file=sys.stderr)
                sys.exit(1)
            print("Response %d from host %s:" % (res.status_code, h), res.json())
            nb_indexed += res.json()["size"]

        print("Indexed %d docs in %d seconds = %.2lf docs / second" % (
            nb_indexed, time() - T0, nb_indexed / (time() - T0)))

        # Reset bookkeeping.
        nb_round_robin_rem = len(es_hosts) * args["batch_size"]
        docs_batch = []
