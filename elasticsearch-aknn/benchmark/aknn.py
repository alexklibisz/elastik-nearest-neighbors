""""""
from argparse import ArgumentParser
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from csv import DictWriter
from itertools import cycle
from more_itertools import chunked
from math import log10
from numpy import array, mean, std, vstack, zeros_like, median
from pprint import pformat, pprint
from sklearn.neighbors import NearestNeighbors
from time import time, sleep
from urllib.parse import quote_plus
import json
import numpy as np
import os
import pdb
import random
import requests
import sys


def iter_local_docs(docs_path, skip=0, stop=sys.maxsize):
    for i, line in enumerate(open(docs_path)):
        if i < skip:
            continue
        elif i < stop:
            yield json.loads(line)
        else:
            break


def iter_es_docs(es_host, es_index, es_type, query={"_source": False}):
    from elasticsearch import Elasticsearch, helpers
    es = Elasticsearch(es_host)
    scroll = helpers.scan(es, query=query, index=es_index, doc_type=es_type)
    for x in scroll:
        yield x
    del es  # No close() method. Guess this works.


def aknn_delete(es_host, es_index):
    print("Deleting index %s" % es_index)
    del_url = "%s/%s" % (es_host, es_index)
    req = requests.delete(del_url)
    if req.status_code == 404:
        print("Index %s doesn't exist" % es_index)
    elif req.status_code == 200:
        print("Deleted index %s" % es_index)
    else:
        raise Exception(req.json())


def aknn_create(docs_path, es_hosts, es_index, es_type, es_id, description,
                nb_dimensions, nb_tables, nb_bits, sample_prob, sample_seed):

    random.seed(sample_seed)

    doc_iterator = iter_local_docs(docs_path)

    req_url = "%s/_aknn_create" % es_hosts[0]
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

    nb_samples = 2 * nb_tables * nb_bits
    print("Collecting %d sample vectors" % (nb_samples))
    while len(body["_aknn_vector_sample"]) < nb_samples:
        doc = next(doc_iterator)
        if random.random() <= sample_prob:
            body["_aknn_vector_sample"].append(doc["_source"]["_aknn_vector"])

    print("Collected samples with mean %.3lf, standard deviation %.3lf" % (
        mean(body["_aknn_vector_sample"]), std(body["_aknn_vector_sample"])))

    print("Posting to Elasticsearch")
    try:
        t0 = time()
        req = requests.post(req_url, json=body)
        req.raise_for_status()
        print("Request completed in %.3lf seconds" % (time() - t0), req.json())
    except requests.exceptions.HTTPError as ex:
        print("Request failed", ex, file=sys.stderr)
        print(ex, file=sys.stderr)
        sys.exit(1)


def aknn_index(docs_path, es_hosts, es_index, es_type, aknn_uri,
               nb_batch, nb_total_max):

    assert es_index.lower() == es_index, "Index must be lowercase."
    assert es_type.lower() == es_type, "Type must be lowercase."

    T0 = time()

    if isinstance(es_hosts, str):
        es_hosts = es_hosts.split(",")
    es_hosts_iter = cycle(es_hosts)
    print("Using %d elasticsearch hosts" % len(es_hosts), es_hosts)

    # Simple iterator over local documents.
    doc_iterator = iter_local_docs(docs_path)

    # Keep a body for each host.
    body = {"_index": es_index,
            "_type": es_type,
            "_aknn_uri": aknn_uri,
            "_aknn_docs": []}

    # Setup for round-robin indexing.
    tpool = ThreadPoolExecutor(max_workers=len(es_hosts))
    nb_round_robin = len(es_hosts) * nb_batch
    nb_total_rem = nb_total_max
    docs_batch = []
    print("Indexing %d new docs" % nb_total_rem)

    while nb_total_rem > 0:

        doc = next(doc_iterator)

        # There are some oddball words in there... ES technically has
        # a limit at strings with length 512 being used for IDs.
        if len(doc["_id"]) > 50:
            continue

        # Add a new doc to the n1ext hosts payload and decrement counters.
        docs_batch.append(doc)
        nb_total_rem -= 1

        # Keep populating payloads if...
        if len(docs_batch) < nb_round_robin and nb_total_rem > 0:
            continue

        # Send each host the payload that it accumulated. Keep result as future.
        futures = []
        for h, docs_batch_chunk in zip(es_hosts, chunked(docs_batch, nb_batch)):
            body["_aknn_docs"] = docs_batch_chunk
            url = "%s/_aknn_index" % h
            futures.append(tpool.submit(requests.post, url, json=body))
            print("POSTed %d docs to host %s" % (len(body["_aknn_docs"]), h))

        # Iterate over the futures and print each host's response.
        # Error if any of the hosts return non-200 status.
        for f, h in zip(as_completed(futures), es_hosts):
            res = f.result()
            if res.status_code != 200:
                print("Error at host %s" % h, res.json(), file=sys.stderr)
                sys.exit(1)
            print("Response %d from host %s:" %
                  (res.status_code, h), res.json())

        # Reset round-robin state.
        docs_batch = []
        print("Indexed %d docs in %d seconds" % (
            nb_total_max - nb_total_rem, time() - T0))

    # Wait for indexing to complete.
    count_url = "%s/%s/%s/_count" % (next(es_hosts_iter), es_index, es_type)
    count = 0
    while count < nb_total_max:
        count = requests.get(count_url).json()["count"]
        print("Checking index, found %d docs" % count)
        sleep(1)


def aknn_search(metrics_path, es_hosts, es_index, es_type, k1, k2,
                nb_requests, nb_workers):

    print("Starting thread pool with %d concurrent workers" % nb_workers)
    tpool = ThreadPoolExecutor(max_workers=nb_workers)

    es_hosts = es_hosts.split(",")
    es_hosts_iter = cycle(es_hosts)

    print("Using %d elasticsearch hosts" % len(es_hosts), es_hosts)

    print("Retrieving IDs in %s/%s for random queries" % (es_index, es_type))
    ids = [doc["_id"] for doc in iter_es_docs(es_hosts[0], es_index, es_type)]

    metrics_fp = open(metrics_path, "w")
    metrics_row = OrderedDict(
        nb_docs=len(ids), nb_workers=nb_workers, nb_requests=nb_requests,
        percent_failed=0, response_min=0, response_max=0,
        response_mean=0, response_median=0, response_stdv=0)
    metrics_writer = DictWriter(metrics_fp, metrics_row.keys())
    metrics_writer.writeheader()

    while True:

        print("Submitting %d searches, k1=%d, k2=%d" % (nb_requests, k1, k2))
        futures, search_urls = [], []
        for id_ in random.sample(ids, nb_requests):
            search_urls.append("%s/%s/%s/%s/_aknn_search" % (
                next(es_hosts_iter), es_index, es_type, quote_plus(id_)))
            futures.append(tpool.submit(requests.get, search_urls[-1]))

        nb_failed = 0
        response_times = []
        for i, f in enumerate(as_completed(futures)):
            res = f.result()
            if res.status_code != 200:
                nb_failed += 1
                print("Error at search: %s" % search_urls[i], res.json(),
                      file=sys.stderr)
                continue
            if i == 0:
                sim_ids = [h["_id"] for h in res.json()["hits"]["hits"]]
                print("Example response: %s" % ", ".join(sim_ids))
            response_times.append(res.json()["took"])

        # Popualte and print metrics.
        metrics_row["percent_failed"] = nb_failed / nb_requests * 100
        metrics_row["response_min"] = min(response_times)
        metrics_row["response_max"] = max(response_times)
        metrics_row["response_mean"] = mean(response_times)
        metrics_row["response_median"] = median(response_times)
        metrics_row["response_stdv"] = std(response_times)
        print("Request metrics....\n%s" % pformat(metrics_row))
        metrics_writer.writerow(metrics_row)

        assert nb_failed < len(futures) * 0.1, "Too many failures, stopping."


def aknn_latency_recall(es_hosts, es_index, es_type, nb_sampled, k1_all, k2, sample_seed=1):

    rng = np.random.RandomState(sample_seed)

    if isinstance(es_hosts, str):
        es_hosts = es_hosts.split(",")
    es_hosts_iter = cycle(es_hosts)

    print("Getting document count for index %s, type %s" % (es_index, es_type))
    count_url = "%s/%s/%s/_count" % (next(es_hosts_iter), es_index, es_type)
    req = requests.get(count_url)
    count = req.json()["count"]

    print("Getting %d vectors from host" % count)
    ids, vecs = [], []
    for doc in iter_es_docs(next(es_hosts_iter), es_index, es_type, {"_source": ["_aknn_vector"]}):
        ids.append(doc["_id"])
        vecs.append(doc["_source"]["_aknn_vector"])

    print("Sampling %d vectors" % nb_sampled)
    sample_ii = rng.permutation(len(ids))[:nb_sampled]
    ids_sample = [ids[i] for i in sample_ii]
    vecs_sample = [vecs[i] for i in sample_ii]

    print("Sampled [%s, ...]" % ", ".join(ids_sample[:10]))

    print("Computing exact KNN")
    model = NearestNeighbors(k2 + 1, algorithm="brute", metric="euclidean")
    _ = model.fit(vecs).kneighbors(vecs_sample, return_distance=False)
    nbrs_ids_true = [[ids[i] for i in ii] for ii in _[:, 1:]]

    # Iterate over k1 values, request neighors, record latency and recall.
    results = []

    for k1 in k1_all:
        print("Computing KNN with k1=%d" % k1)
        results.append(dict(k1=k1, k2=k2, recalls=[], latencies=[]))
        for id_, nbrs_ids_true_ in zip(ids_sample, nbrs_ids_true):
            search_url = "%s/%s/%s/%s/_aknn_search?k1=%d&k2=%d" % (
                next(es_hosts_iter), es_index, es_type, quote_plus(id_), k1, k2 + 1)
            req = requests.get(search_url)
            nbrs_ids_pred = [x["_id"] for x in req.json()["hits"]["hits"][1:]]
            rec = len(np.intersect1d(nbrs_ids_true_, nbrs_ids_pred)) / k2
            results[-1]["recalls"].append(rec)
            results[-1]["latencies"].append(req.json()["took"])

    return results


if __name__ == "__main__":

    ap = ArgumentParser(description="Elasticsearch-Aknn CLI")
    ap.add_argument("--es_hosts", default="http://localhost:9200",
                    help="comma-separated list of elasticsearch endpoints")

    sp_base = ap.add_subparsers(
        title='actions', description='Choose an action')

    sp_c = sp_base.add_parser("create")
    sp_c.set_defaults(which="create")
    sp_c.add_argument("docs_path", type=str)
    sp_c.add_argument("--sample_prob", type=float, default=0.3)
    sp_c.add_argument("--sample_seed", type=int, default=865)
    sp_c.add_argument("--es_index", type=str, default="aknn_models")
    sp_c.add_argument("--es_type", type=str, default="aknn_model")
    sp_c.add_argument("--es_id", type=str, required=True)
    sp_c.add_argument("--description", type=str, required=True)
    sp_c.add_argument("--nb_dimensions", type=int, required=True)
    sp_c.add_argument("--nb_tables", type=int, default=50)
    sp_c.add_argument("--nb_bits", type=int, default=16)

    sp_i = sp_base.add_parser("index")
    sp_i.set_defaults(which="index")
    sp_i.add_argument("docs_path", type=str)
    sp_i.add_argument("--aknn_uri", type=str, required=True)
    sp_i.add_argument("--es_index", type=str, required=True)
    sp_i.add_argument("--es_type", type=str, required=True)
    sp_i.add_argument("--nb_batch", type=int, default=5000)
    sp_i.add_argument("--nb_total_max", type=int, default=100000)
    sp_i.add_argument("--skip_existing", action="store_true")

    # sp_r = sp_base.add_parser("recall")
    # sp_r.set_defaults(which="recall")
    # sp_r.add_argument("docs_path", type=str)
    # sp_r.add_argument("metrics_dir", default="metrics", type=str)
    # sp_r.add_argument("--es_index", type=str, required=True)
    # sp_r.add_argument("--es_type", type=str, required=True)
    # sp_r.add_argument("--nb_measured", type=int, default=1000)
    # sp_r.add_argument("--k1", default="10,100,1000")
    # sp_r.add_argument("--k2", type=int, default=10)
    # sp_r.add_argument("--sample_seed", type=int, default=865)

    sp_s = sp_base.add_parser("search")
    sp_s.set_defaults(which="search")
    sp_s.add_argument("metrics_path", type=str)
    sp_s.add_argument("--es_index", type=str, required=True)
    sp_s.add_argument("--es_type", type=str, required=True)
    sp_s.add_argument("--k1", type=int, default=100)
    sp_s.add_argument("--k2", type=int, default=10)
    sp_s.add_argument("--nb_workers", type=int, default=20)
    sp_s.add_argument("--nb_requests", type=int, default=100)

    args = vars(ap.parse_args())
    pprint(args)

    action = args["which"]
    del args["which"]

    if action == "create":
        aknn_create(**args)

    if action == "index":
        aknn_index(**args)

    if action == "search":
        aknn_search(**args)

    if action == "recall":
        aknn_recall(**args)
