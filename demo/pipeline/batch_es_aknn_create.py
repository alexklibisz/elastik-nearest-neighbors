"""Create Elasticsearch-aknn model from feature documents on disk or S3.
"""

from argparse import ArgumentParser
from io import BytesIO
from numpy import mean, std
from pprint import pformat
from sys import stderr
from time import time
import boto3
import json
import gzip
import os
import pdb
import random
import requests


def iter_feature_docs(source_str):

    if source_str.startswith("s3://"):
        bucket = boto3.resource("s3").Bucket(source_str.replace("s3://", ''))
        for obj in bucket.objects.all():
            body = obj.get().get('Body')
            buff = BytesIO(body.read())
            with gzip.open(buff) as fp:
                yield json.loads(fp.read().decode())
    else:
        for fobj in os.scandir(source_str):
            with gzip.open(fobj.path) as fp:
                yield json.loads(fp.read().decode())


if __name__ == "__main__":

    ap = ArgumentParser(description="See script")
    ap.add_argument("features_source",
                    help="Directory or S3 bucket containing image feature docs.")
    ap.add_argument("--es_host", default="http://localhost:9200",
                    help="URL of single elasticsearch server.")
    ap.add_argument("--aknn_tables", type=int, default=16)
    ap.add_argument("--aknn_bits", type=int, default=8)
    ap.add_argument("--aknn_dimensions", type=int, default=1000)
    ap.add_argument("-p", type=float, default=0.5,
                    help="Prob. of accepting a feature document as a sample.")
    args = vars(ap.parse_args())

    # Prepare the document structure.
    model_doc = {
        "_index": "aknn_models",
        "_type": "aknn_model",
        "_id": "twitter_images",
        "_source": {
            "_aknn_description": "AKNN model for images on the twitter public stream",
            "_aknn_nb_dimensions": args["aknn_dimensions"],
            "_aknn_nb_tables": args["aknn_tables"],
            "_aknn_bits_per_table": args["aknn_bits"]
        },
        "_aknn_vector_sample": [
            # Populated below.
        ]
    }

    # Create an iterable over the feature documents.
    feature_docs = iter_feature_docs(args["features_source"])

    # Populate the vector sample by randomly sampling vectors from iterable.
    nb_samples = 2 * args["aknn_bits"] * args["aknn_tables"]
    print("Taking sample of %d feature vectors from %s" % (
        nb_samples, args["features_source"]))
    while len(model_doc["_aknn_vector_sample"]) < nb_samples:
        vec = next(feature_docs)["feature_vector"]
        if random.random() <= args["p"]:
            model_doc["_aknn_vector_sample"].append(vec)

    print("Sample mean, std = %.3lf, %.3lf" % (
        mean(model_doc["_aknn_vector_sample"]),
        std(model_doc["_aknn_vector_sample"])))

    print("Posting to Elasticsearch")
    t0 = time()
    res = requests.post("%s/_aknn_create" % args["es_host"], json=model_doc)
    if res.status_code == requests.codes.ok:
        print("Successfully built model in %d seconds" % (time() - t0))
        print(pformat(res.json()))
    else:
        print("Failed with error code %d" % res.status_code, file=stderr)
