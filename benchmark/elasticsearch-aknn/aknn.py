""""""
from argparse import ArgumentParser
from csv import DictWriter
from itertools import cycle
from numpy import mean, std
from pprint import pformat, pprint
from time import time
import json
import os
import pdb
import random
import requests
import sys


def iter_over_docs(docs_path):
    for line in open(docs_path):
        yield json.loads(line)


def aknn_create(doc_iterator, es_hosts, es_index, es_type, es_id, description,
                nb_dimensions, nb_tables, nb_bits, prob_sample):

    es_url = es_hosts.split(",")[0]
    req_url = "%s/_aknn_create" % es_url
    body = {
        "_index": es_index,
        "_type": es_type,
        "_id": es_id,
        "_source": {
            "_aknn_description": description,
            "_aknn_nb_tables": nb_tables,
            "_aknn_nb_bits_per_table": nb_bits,
            "_aknn_nb_dimensions": nb_dimensions
        },
        "_aknn_vector_sample": []
    }

    print("Collecting %d sample vectors" % (2 * nb_tables * nb_bits))
    while len(body["_aknn_vector_sample"]) < 2 * nb_tables * nb_bits:
        doc = next(doc_iterator)
        if random.random() <= prob_sample:
            body["_aknn_vector_sample"].append(doc["_source"]["_aknn_vector"])

    print("Collected samples with mean %.3lf, standard deviation %.3lf" % (
        mean(body["_aknn_vector_sample"]), std(body["_aknn_vector_sample"])))

    print("Posting to Elasticsearch")
    try:
        t0 = time()
        req = requests.post(req_url, json=body)
        req.raise_for_status()
        print("Request completed in %.3lf seconds" % (time() - t0))
        pprint(req.json())
    except requests.exceptions.HTTPError as ex:
        print("Request failed", file=sys.stderr)
        print(ex, file=sys.stderr)
        sys.exit(1)


def aknn_index(doc_iterator, es_hosts, es_index, es_type, aknn_uri, batch_size, count, metrics_path):

    T0 = time()
    body = {
        "_index": es_index,
        "_type": es_type,
        "_aknn_uri": aknn_uri,
        "_aknn_docs": []
    }

    es_hosts = cycle(es_hosts.split(","))
    fp_metrics = open(metrics_path, "w")

    metrics_cols = ["nb_total", "nb_batch", "request_sec", "elapsed_sec"]
    metrics_writer = DictWriter(fp_metrics, metrics_cols)
    metrics_writer.writeheader()
    metrics_row = {x: 0 for x in metrics_cols}

    for doc in doc_iterator:
        body["_aknn_docs"].append(doc)

        if len(body["_aknn_docs"]) == batch_size:
            t0 = time()
            try:
                req_url = "%s/_aknn_index" % next(es_hosts)
                req = requests.post(req_url, json=body)
                req.raise_for_status()
            except requests.exceptions.HTTPError as ex:
                print("Request failed", file=sys.stderr)
                print(ex, file=sys.stderr)
                sys.exit(1)
            body["_aknn_docs"] = []
            metrics_row["nb_total"] += batch_size
            metrics_row["nb_batch"] = batch_size
            metrics_row["request_sec"] = round(time() - t0, 5)
            metrics_row["elapsed_sec"] = round(time() - T0, 5)
            metrics_writer.writerow(metrics_row)
            print(metrics_row)

        if metrics_row["nb_total"] >= count:
            print("Done")
            return

if __name__ == "__main__":

    ap = ArgumentParser(description="Minimalistic Elasticsearch-Aknn CLI")
    ap.add_argument("-e", "--elastic_hosts", default="http://localhost:9200",
                    help="comma-separated list of elasticsearch endpoints")

    sp_base = ap.add_subparsers(title='actions', description='Choose an action')

    sp_c = sp_base.add_parser("create")
    sp_c.set_defaults(which="create")
    sp_c.add_argument("docs_path", type=str)
    sp_c.add_argument("--prob_sample", type=float, default=0.5)
    sp_c.add_argument("--index", type=str, default="aknn_models")
    sp_c.add_argument("--type", type=str, default="aknn_model")
    sp_c.add_argument("--id", type=str, required=True)
    sp_c.add_argument("--description", type=str, required=True)
    sp_c.add_argument("--dimensions", type=int, required=True)
    sp_c.add_argument("--tables", type=int, default=16)
    sp_c.add_argument("--bits", type=int, default=16)

    sp_i = sp_base.add_parser("index")
    sp_i.set_defaults(which="index")
    sp_i.add_argument("docs_path", type=str)
    sp_i.add_argument("metrics_path", type=str)
    sp_i.add_argument("--aknn_uri", type=str, required=True)
    sp_i.add_argument("--index", type=str, required=True)
    sp_i.add_argument("--type", type=str, required=True)
    sp_i.add_argument("--batch_size", type=int, default=10000)
    sp_i.add_argument("--count", type=int, required=True)

    sp_s = sp_base.add_parser("search")
    sp_s.set_defaults(which="search")
    sp_s.add_argument("--index", type=str, required=True)
    sp_s.add_argument("--type", type=str, required=True)
    sp_s.add_argument("--time", type=int, default=10)
    sp_s.add_argument("--threads", type=int, default=10)

    args = vars(ap.parse_args())

    if args["which"] == "create":
        aknn_create(iter_over_docs(args["docs_path"]),
                    args["elastic_hosts"],
                    args["index"], args["type"], args["id"],
                    args["description"], args["dimensions"],
                    args["tables"], args["bits"], args["prob_sample"])

    if args["which"] == "index":
        aknn_index(iter_over_docs(args["docs_path"]),
                   args["elastic_hosts"],
                   args["index"], args["type"],
                   args["aknn_uri"], args["batch_size"], args["count"],
                   args["metrics_path"])
