""""""
from argparse import ArgumentParser
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from csv import DictWriter
from itertools import cycle
from math import log10
from numpy import array, mean, std, vstack, zeros_like, median
from pprint import pformat, pprint
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


def aknn_create(docs_path, es_hosts, es_index, es_type, es_id, description,
                nb_dimensions, nb_tables, nb_bits, sample_prob, sample_seed):

    random.seed(sample_seed)

    doc_iterator = iter_local_docs(docs_path)

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
        req.raise_for_status()
        print("Request completed in %.3lf seconds" % (time() - t0))
        pprint(req.json())
    except requests.exceptions.HTTPError as ex:
        print("Request failed", ex, file=sys.stderr)
        print(ex, file=sys.stderr)
        sys.exit(1)


def aknn_index(docs_path, es_hosts, es_index, es_type, aknn_uri,
               nb_batch, nb_total_max, skip_existing):

    assert es_index.lower() == es_index, "Index must be lowercase."
    assert es_type.lower() == es_type, "Type must be lowercase."

    T0 = time()

    es_hosts = es_hosts.split(",")
    es_hosts_iter = cycle(es_hosts)
    print("Using %d elasticsearch hosts" % len(es_hosts), es_hosts)

    # Get the existing ids from the
    ids_in_index = set([])
    if skip_existing:
        for doc in iter_es_docs(next(es_hosts_iter), es_index, es_type):
            ids_in_index.add(doc["_id"])
        print("Found %d existing ids" % len(ids_in_index))

    # Based on number already in index, determine how many more to index.
    nb_total_rem = nb_total_max - len(ids_in_index)
    print("Indexing %d new documents" % nb_total_rem)

    # Simple iterator over local documents.
    doc_iterator = iter_local_docs(docs_path)

    # Keep a body for each host.
    bodies = {
        h: {
            "_index": es_index,
            "_type": es_type,
            "_aknn_uri": aknn_uri,
            "_aknn_docs": []
        } for h in es_hosts}

    nb_round_robin_rem = len(es_hosts) * nb_batch

    # Thread pool for parallelized round-robin requests.
    tpool = ThreadPoolExecutor(max_workers=len(es_hosts))

    while nb_total_rem > 0:

        # Skip include duplicate docs.
        doc = next(doc_iterator)
        if doc["_id"] in ids_in_index:
            continue

        # There are some oddball words in there... ES technically has
        # a limit at strings with length 512 being used for IDs.
        if len(doc["_id"]) > 50:
            continue

        # Add a new doc to the next hosts payload and decrement counters.
        bodies[next(es_hosts_iter)]["_aknn_docs"].append(doc)
        nb_round_robin_rem -= 1
        nb_total_rem -= 1

        # Keep populating payloads if satisfying...
        if nb_round_robin_rem > 0 and nb_total_rem > 0:
            continue

        # Send each host the payload that it accumulated. Keep result as future.
        futures = []
        for h in es_hosts:
            body = bodies[h]
            url = "%s/_aknn_index" % h
            print("Sending %d docs to host %s" % (len(body["_aknn_docs"]), h))
            futures.append(tpool.submit(requests.post, url, json=body))

        # Iterate over the futures and print each host's response.
        # Error if any of the hosts return non-200 status.
        for f, h in zip(as_completed(futures), es_hosts):
            res = f.result()
            if res.status_code != 200:
                print("Error at host %s" % h, res.json(), file=sys.stderr)
                sys.exit(1)
            print("Response from host %s:" % h, res.json())

        # Reset counter and empty payloads.
        nb_round_robin_rem = len(es_hosts) * nb_batch
        for h in es_hosts:
            bodies[h]["_aknn_docs"] = []

        print("Total indexed docs = %d, seconds elapsed = %d" % (
            nb_total_max - nb_total_rem, time() - T0))


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


def aknn_recall(docs_path, metrics_dir, es_hosts, es_index, es_type,
                nb_measured, k1, k2, sample_seed):
    """Compute recall for exact KNN and several settings of approximate KNN."""

    from sklearn.neighbors import NearestNeighbors
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    random.seed(sample_seed)

    es_hosts = cycle(es_hosts.split(","))

    # Get number of documents for this index/type.
    nb_existing_url = "%s/%s/%s/_count" % (next(es_hosts), es_index, es_type)
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
    for doc in iter_local_docs(docs_path, 0, nb_existing):
        ids.append(doc["_id"])
        vecs.append(array(doc["_source"]["_aknn_vector"]).astype('float32'))
    vecs = vstack(vecs)

    print("Sampling %d random ids and vectors to measure" % nb_measured)
    measured_ind = random.sample(range(nb_existing), nb_measured)
    measured_ids = [ids[i] for i in measured_ind]

    boxplot_data = []
    thread_pool = ThreadPoolExecutor(max_workers=20)

    print("Computing exact KNN")
    knn = NearestNeighbors(k2, algorithm='brute',
                           metric='euclidean').fit(vecs)
    nbrs_inds = knn.kneighbors(vecs[measured_ind], return_distance=False)
    id_to_nbr_ids = {}
    for id_, nbrs_inds_ in zip(measured_ids, nbrs_inds):
        id_to_nbr_ids[id_] = [ids[i] for i in nbrs_inds_]

    boxplot_data = []

    for k1_ in map(int, k1.split(",")):

        print("Searching approximate KNN for k1=%d" % k1_)
        search_urls = []
        for i, id_ in enumerate(measured_ids):
            search_urls.append(
                "%s/%s/%s/%s/_aknn_search?k1=%d&k2=%d" % (
                    next(es_hosts), es_index, es_type, quote_plus(id_), k1_, k2))

        reqs = list(thread_pool.map(requests.get, search_urls))
        recalls = []
        for id_, req in zip(measured_ids, reqs):
            nbr_ids_exact = id_to_nbr_ids[id_]
            nbr_ids_approx = [x["_id"] for x in req.json()["hits"]["hits"]]
            nb_intersect = len(set(nbr_ids_exact).intersection(nbr_ids_approx))
            recalls.append(nb_intersect / k2)

        boxplot_data.append(recalls)

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

    xticks = ["$10^%d$" % log10(int(x)) for x in k1.split(",")]
    plt.xticks(range(1, len(boxplot_data) + 1), xticks, fontsize=15)
    plt.yticks(fontsize=15)
    plt.xlabel("\nNumber of Distance Computations", size=18)
    plt.ylabel("Recall @ %d" % k2, size=18)

    fig_path = "%s/recall_boxplot.png" % metrics_dir
    plt.savefig(fig_path, bbox_inches='tight', pad_inches=0.1)
    print("Saved figure at %s" % fig_path)


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
