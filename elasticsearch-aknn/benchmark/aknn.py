""""""
from argparse import ArgumentParser
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait
from csv import DictWriter
from itertools import cycle
from math import log10
from numpy import array, mean, std, vstack, zeros_like, median
from pprint import pformat, pprint
from sklearn.neighbors import NearestNeighbors
from time import time
from urllib.parse import quote_plus
import json
import os
import pdb
import random
import requests
import sys


def values_to_cdf(values):
    """Compute the CDF for a list of values."""
    cntr = Counter(values)
    values_unique_sorted = sorted(cntr.keys())
    prlt = []
    nblt = 0
    for val in values_unique_sorted:
        nblt += cntr[val]
        prlt.append(nblt / len(values))
    return values_unique_sorted, prlt


def iter_over_docs(docs_path, skip=0, stop=sys.maxsize):
    for i, line in enumerate(open(docs_path)):
        if i < skip:
            continue
        elif i < stop:
            yield json.loads(line)
        else:
            break


def aknn_create(docs_path, es_hosts, es_index, es_type, es_id, description,
                nb_dimensions, nb_tables, nb_bits, sample_prob, sample_seed):

    random.seed(sample_seed)

    doc_iterator = iter_over_docs(docs_path)

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

    nb_samples = 2 * nb_tables * nb_bits
    print("Collecting %d sample vectors with probability %.2lf, seed %d" % (
        nb_samples, sample_prob, sample_seed))
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
        pdb.set_trace()
        req.raise_for_status()
        print("Request completed in %.3lf seconds" % (time() - t0))
        pprint(req.json())
    except requests.exceptions.HTTPError as ex:
        pdb.set_trace()
        print("Request failed", ex, file=sys.stderr)
        print(ex, file=sys.stderr)
        sys.exit(1)


def aknn_index(docs_path, metrics_path, es_hosts, es_index, es_type, aknn_uri,
               nb_batch, nb_total_max):

    T0 = time()
    body = {
        "_index": es_index,
        "_type": es_type,
        "_aknn_uri": aknn_uri,
        "_aknn_docs": []
    }

    es_hosts = cycle(es_hosts.split(","))

    # Prepare metrics csv writer.
    fp_metrics = open(metrics_path, "w")
    metrics_cols = ["nb_total", "nb_batch", "request_sec", "elapsed_sec"]
    metrics_writer = DictWriter(fp_metrics, metrics_cols)
    metrics_writer.writeheader()
    metrics_row = {x: 0 for x in metrics_cols}

    # Get number of documents for this index/type.
    nb_existing_url = "%s/%s/%s/_count" % (next(es_hosts), es_index, es_type)
    try:
        req = requests.get(nb_existing_url)
        req.raise_for_status()
        nb_existing = req.json()["count"]
    except requests.exceptions.HTTPError as ex:
        print("Request for existing count failed", ex, file=sys.stderr)
        nb_existing = 0

    # Skip the existing documents in the file.
    print("Skipping %d existing docs" % nb_existing)
    print("Indexing %d new docs" % (nb_total_max - nb_existing))
    doc_iterator = iter_over_docs(docs_path, nb_existing, nb_total_max)

    for doc in doc_iterator:
        body["_aknn_docs"].append(doc)

        if len(body["_aknn_docs"]) == nb_batch:
            print("Posting %d docs: [%s ... %s]" % (
                nb_batch, body["_aknn_docs"][0]["_id"], body["_aknn_docs"][-1]["_id"]))
            t0 = time()
            try:
                req_url = "%s/_aknn_index" % next(es_hosts)
                pdb.set_trace()
                req = requests.post(req_url, json=body)
                req.raise_for_status()
            except requests.exceptions.HTTPError as ex:
                print("Request failed", file=sys.stderr)
                print(ex, file=sys.stderr)
                sys.exit(1)
            body["_aknn_docs"] = []
            metrics_row["nb_total"] += nb_batch
            metrics_row["nb_batch"] = nb_batch
            metrics_row["request_sec"] = round(time() - t0, 5)
            metrics_row["elapsed_sec"] = round(time() - T0, 5)
            metrics_writer.writerow(metrics_row)
            print(metrics_row)


def aknn_recall(docs_path, metrics_dir, es_hosts, es_index, es_type, nb_measured,
                k1, k2, sample_seed):
    """Compare the distribution of euclidean distances returned for an exact
    KNN and for various settings of Elasticsearch-Aknn."""

    import matplotlib.pyplot as plt
    random.seed(sample_seed)

    # Get number of documents for this index/type.
    nb_existing_url = "%s/%s/%s/_count" % (es_hosts, es_index, es_type)
    try:
        req = requests.get(nb_existing_url)
        req.raise_for_status()
        nb_existing = req.json()["count"]
    except requests.exceptions.HTTPError as ex:
        print("Request for existing count failed", ex, file=sys.stderr)
        nb_existing = 0

    print("Found %d documents in Elasticsearch" % nb_existing)
    assert nb_measured <= nb_existing

    # Compile all ids and vectors from elasticsearch into a numpy array.
    print("Reading first %d ids and vectors into memory" % nb_existing)
    ids, vecs = [], []
    for doc in iter_over_docs(docs_path, 0, nb_existing):
        ids.append(doc["_id"])
        vecs.append(array(doc["_source"]["_aknn_vector"]).astype('float32'))
    vecs = vstack(vecs)

    print("Sampling %d random ids and vectors to measure" % nb_measured)
    measured_ind = random.sample(range(nb_existing), nb_measured)
    measured_ids = [ids[i] for i in measured_ind]

    boxplot_data = []
    thread_pool = ThreadPoolExecutor(max_workers=20)

    for k1_ in map(int, k1.split(",")):

        print("Computing approximate KNN for k1=%d" % k1_)
        search_urls = []
        for i, id_ in enumerate(measured_ids):
            search_urls.append(
                "%s/%s/%s/%s/_aknn_search?k1=%d&k2=%d" % (
                    es_hosts, es_index, es_type, quote_plus(id_), k1_, k2))

        reqs = list(thread_pool.map(requests.get, search_urls))
        dd = []
        for i, req in enumerate(reqs):
            dd += [x["_score"] for x in req.json()["hits"]["hits"][1:]]

        print("Approx mean, std = (%.3lf, %.3lf)" % (mean(dd), std(dd)))
        boxplot_data.append(dd)

    print("Computing exact KNN")
    knn = NearestNeighbors(k2, algorithm='brute', metric='euclidean').fit(vecs)
    dd, _ = knn.kneighbors(vecs[measured_ind], return_distance=True)
    dd = dd[:, 1:].ravel().tolist()
    print("Exact mean, std =  (%.3lf, %.3lf)" % (mean(dd), std(dd)))
    boxplot_data.append(dd)

    plt.figure(figsize=(20, 10))
    plt.grid(True)
    bp = plt.boxplot(boxplot_data, notch=True, patch_artist=True)

    colors = ['#FF595E', '#FFCA3A', '#1982C4', '#8AC926', '#57E2E5', '#6A4C93']
    assert len(boxplot_data) <= len(colors), "Add more colors..."
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)

    for x in bp["medians"]:
        x.set(color="black")

    for k in ["boxes", "medians", "whiskers", "caps"]:
        for x in bp[k]:
            x.set(linewidth=4)

    xticks = ["$10^%d$" % log10(int(x)) for x in k1.split(",") + [nb_existing]]
    plt.xticks(range(1, len(boxplot_data) + 1), xticks, fontsize=15)
    plt.yticks(fontsize=15)
    plt.xlabel("\nNumber of Distance Computations", size=18)
    plt.ylabel("Euclidean Distance to Neighbors", size=18)
    plt.title("For each of %d vectors, find the %d nearest neighbors in a corpus of $10^%d$ vectors" % (
        nb_measured, k2, log10(nb_existing)), size=22, y=1.03)

    fig_path = "%s/recall_boxplot.png" % metrics_dir
    plt.savefig(fig_path, bbox_inches='tight', pad_inches=0.1)
    print("Saved figure at %s" % fig_path)

if __name__ == "__main__":

    ap = ArgumentParser(description="Elasticsearch-Aknn CLI")
    ap.add_argument("-e", "--es_hosts", default="http://localhost:9200",
                    help="comma-separated list of elasticsearch endpoints")

    sp_base = ap.add_subparsers(title='actions', description='Choose an action')

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
    sp_c.add_argument("--nb_tables", type=int, default=16)
    sp_c.add_argument("--nb_bits", type=int, default=16)

    sp_i = sp_base.add_parser("index")
    sp_i.set_defaults(which="index")
    sp_i.add_argument("docs_path", type=str)
    sp_i.add_argument("metrics_path", type=str)
    sp_i.add_argument("--aknn_uri", type=str, required=True)
    sp_i.add_argument("--es_index", type=str, required=True)
    sp_i.add_argument("--es_type", type=str, required=True)
    sp_i.add_argument("--nb_batch", type=int, default=5000)
    sp_i.add_argument("--nb_total_max", type=int, default=100000)

    sp_s = sp_base.add_parser("search")
    sp_s.set_defaults(which="search")
    sp_s.add_argument("--index", type=str, required=True)
    sp_s.add_argument("--type", type=str, required=True)
    sp_s.add_argument("--time", type=int, default=10)
    sp_s.add_argument("--threads", type=int, default=10)

    sp_r = sp_base.add_parser("recall")
    sp_r.set_defaults(which="recall")
    sp_r.add_argument("docs_path", type=str)
    sp_r.add_argument("metrics_dir", default="metrics", type=str)
    sp_r.add_argument("--es_index", type=str, required=True)
    sp_r.add_argument("--es_type", type=str, required=True)
    sp_r.add_argument("--nb_measured", type=int, default=1000)
    sp_r.add_argument("--k1", default="10,100,1000")
    sp_r.add_argument("--k2", type=int, default=10)
    sp_r.add_argument("--sample_seed", type=int, default=865)

    args = vars(ap.parse_args())
    pprint(args)

    action = args["which"]
    del args["which"]

    if action == "create":
        aknn_create(**args)

    if action == "index":
        aknn_index(**args)

    if action == "search":
        raise NotImplementedException("TODO: implement parallelized searching")

    if action == "recall":
        aknn_recall(**args)
