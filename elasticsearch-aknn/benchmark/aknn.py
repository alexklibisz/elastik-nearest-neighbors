"""
This is a driver script for interacting with the Elasticsearch-Aknn plugin.
"""
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from elasticsearch import Elasticsearch, helpers
from itertools import cycle, product
from more_itertools import chunked
from pprint import pformat, pprint
from sklearn.neighbors import NearestNeighbors
from time import time, sleep
from urllib.parse import quote_plus
import elasticsearch
import json
import numpy as np
import os
import pdb
import random
import requests
import sys


def iter_local_docs(docs_path, skip=0, stop=sys.maxsize):
    """Iterate over docs stored in a local file.
    Docs should be JSON formatted, one per line in the file."""
    for i, line in enumerate(open(docs_path)):
        if i < skip:
            continue
        elif i < stop:
            yield json.loads(line)
        else:
            break


def iter_es_docs(es_host, es_index, es_type, query={"_source": False}):
    """Iterate over docs stored in elasticsearch."""
    es = Elasticsearch(es_host)
    scroll = helpers.scan(es, query=query, index=es_index, doc_type=es_type)
    try:
        for x in scroll:
            yield x
    except elasticsearch.exceptions.NotFoundError as ex:
        print(ex, file=sys.stderr)
    finally:
        del es  # No close() method. Guess this works.


def parse_hosts(es_hosts):
    if isinstance(es_hosts, str):
        es_hosts = es_hosts.split(",")
    return es_hosts, cycle(es_hosts)


def aknn_create(docs_path, es_hosts, es_index, es_type, es_id, description, nb_dimensions, nb_tables, nb_bits, sample_prob, sample_seed):

    random.seed(sample_seed)

    es_hosts, _ = parse_hosts(es_hosts)

    print("Creating model at %s/%s/%s with %d dimensions, %d tables, %d bits" % (
        es_index, es_type, es_id, nb_dimensions, nb_tables, nb_bits))

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
        np.mean(body["_aknn_vector_sample"]), np.std(body["_aknn_vector_sample"])))

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


def aknn_index(docs_path, es_hosts, es_index, es_type, aknn_uri, nb_batch, nb_total_max, skip_existing):

    assert es_index.lower() == es_index, "Index must be lowercase."
    assert es_type.lower() == es_type, "Type must be lowercase."

    print("Indexing %d docs at %s/%s using model at %s" % (
        nb_total_max, es_index, es_type, aknn_uri))

    T0 = time()

    es_hosts, es_hosts_c = parse_hosts(es_hosts)
    print("Using %d elasticsearch hosts" % len(es_hosts), es_hosts)

    # Optionally determine which ids will be skipped.
    skip_ids = set([])
    if skip_existing:
        for doc in iter_es_docs(es_hosts[0], es_index, es_type):
            skip_ids.add(doc["_id"])
    print("Skipping %d existing docs" % len(skip_ids))

    # Simple iterator over local documents.
    doc_iterator = iter_local_docs(docs_path)

    # Structure of the body posted.
    body = {"_index": es_index,
            "_type": es_type,
            "_aknn_uri": aknn_uri,
            "_aknn_docs": []}

    # Setup for round-robin indexing.
    tpool = ThreadPoolExecutor(max_workers=len(es_hosts))
    nb_round_robin = len(es_hosts) * nb_batch
    nb_total_rem = nb_total_max - len(skip_ids)
    docs_batch = []
    print("Indexing %d new docs" % nb_total_rem)

    # List of server times will be returned.
    times = []

    while nb_total_rem > 0:

        doc = next(doc_iterator)

        # Skip if it already exists in index.
        if doc["_id"] in skip_ids:
            continue

        # Add a new doc to the payload and decrement counters.
        docs_batch.append(doc)
        nb_total_rem -= 1

        # Keep populating payloads if...
        if len(docs_batch) < nb_round_robin and nb_total_rem > 0:
            continue

        # Send batches of docs to the hosts.
        futures = []
        for h, docs_batch_chunk in zip(es_hosts, chunked(docs_batch, nb_batch)):
            body["_aknn_docs"] = docs_batch_chunk
            url = "%s/_aknn_index" % h
            futures.append(tpool.submit(requests.post, url, json=body))
            print("Posted %d docs to host %s" % (len(body["_aknn_docs"]), h))

        # Iterate over the futures and print each host's response.
        # Error if any of the hosts return non-200 status.
        for f, h in zip(as_completed(futures), es_hosts):
            res = f.result()
            if res.status_code != 200:
                print("Error at host %s" % h, res.json(), file=sys.stderr)
                sys.exit(1)
            print("Response %d from host %s: %s" %
                  (res.status_code, h, res.json()))
            times.append(res.json()["took"])

        # Reset round-robin state.
        docs_batch = []
        print("Indexed %d docs in %d seconds" % (
            nb_total_max - nb_total_rem, time() - T0))

    # Wait for indexing to complete by checking the count in a loop.
    count_url = "%s/%s/%s/_count" % (next(es_hosts_c), es_index, es_type)
    count, t0 = 0, time()
    while count < nb_total_max:
        if time() - t0 > 30:
            print("Timed out", sys.stderr)
            sys.exit(1)
        count = requests.get(count_url).json()["count"]
        print("Checking index, found %d docs" % count)
        sleep(1)

    return times


def aknn_search(es_hosts, es_index, es_type, es_ids, k1, k2, nb_requests, nb_workers):

    print("Starting thread pool with %d concurrent workers" % nb_workers)
    tpool = ThreadPoolExecutor(max_workers=nb_workers)

    es_hosts, es_hosts_c = parse_hosts(es_hosts)
    print("Using %d elasticsearch hosts" % len(es_hosts), es_hosts)

    times, hits = [], []

    print("Submitting %d searches, k1=%d, k2=%d" % (nb_requests, k1, k2))
    futures, search_urls = [], []
    for id_ in es_ids:
        search_urls.append("%s/%s/%s/%s/_aknn_search?k1=%d&k2=%d" % (
            next(es_hosts_c), es_index, es_type, quote_plus(id_), k1, k2))
        futures.append(tpool.submit(requests.get, search_urls[-1]))

    for i, f in enumerate(as_completed(futures)):
        res = f.result()
        if res.status_code != 200:
            print("Error at search: %s" % search_urls[i], res.json(),
                  file=sys.stderr)
            sys.exit(1)
        if i == 0:
            print("Example response: %s" % pformat(res.json()["hits"]["hits"]))
        times.append(res.json()["took"])
        hits.append(res.json()["hits"]["hits"])

    return times, hits


def aknn_benchmark(es_hosts, docs_path, metrics_dir, nb_dimensions, nb_batch, nb_eval, k2, confirm_destructive):

    # Running this part of the script requires deleting and re-creating multiple indexes.
    # This could mess up the user's existing indexes and documents, so make
    # sure to confirm.
    if not confirm_destructive:
        confirm = input(
            "This script deletes indexes on the specified Elasticsearch hosts, continue? [y/n] ")
        if confirm.lower() != "y":
            print("Exiting", file=sys.stderr)
            sys.exit(1)

    T0 = time()

    # Define structure for metrics file.
    metrics = {
        "nb_docs": 0,
        "nb_tables": 0,
        "nb_bits": 0,
        "k1": 0,
        "k2": k2,
        "nb_batch": nb_batch,
        "index_times": [],
        "search_times": [],
        "search_recalls": []
    }

    def get_metrics_path(m):
        return "%s/metrics_%d_%d_%d_%d_%d.json" % (
            metrics_dir, m["nb_docs"], m["nb_tables"], m["nb_bits"], m["k1"], m["k2"])

    # Define model index/type and vectors' index/type.
    model_index = "bench_aknn_models"
    model_type = "aknn_model"
    vecs_index = "vecs"  # Set below..
    vecs_type = "vec"

    # Define the space of parameters to test. If you change any of these
    # parameters, it's safest to delete all of the metrics files.
    nb_docs_space = [10 ** 4, 10 ** 5, int(5 * 10**5), 10 ** 6]
    nb_tables_space = [10, 50, 100, 200]
    nb_bits_space = [8, 12, 16, 20]
    k1_space = [int(k2 * 1.5), k2 * 10, k2 * 100]

    # One test for each combination of parameters.
    nb_tests_total = len(nb_tables_space) * \
        len(nb_bits_space) * len(nb_docs_space) * len(k1_space)
    nb_tests_done = 0

    # For efficiency nb_docs should be sorted.
    assert nb_docs_space == sorted(nb_docs_space)

    # Parse elasticsearch hosts.
    es_hosts, es_hosts_c = parse_hosts(es_hosts)

    for nb_tables, nb_bits in product(nb_tables_space, nb_bits_space):

        # Delete all of the existing vectors for this (tables, bits) config.
        rq = requests.delete("%s/%s" % (next(es_hosts_c), vecs_index))

        # Create an aknn model for this config.
        model_id = "model_%d_%d" % (nb_tables, nb_bits)
        aknn_create(
            docs_path=docs_path,
            es_hosts=es_hosts,
            es_index=model_index,
            es_type=model_type,
            es_id=model_id,
            description="Benchmark: %d tables, %d bits" % (nb_tables, nb_bits),
            nb_dimensions=nb_dimensions,
            nb_tables=nb_tables,
            nb_bits=nb_bits,
            sample_prob=0.3,
            sample_seed=865)

        # Run tests for each corpus size. Add to the corpus incrementally.
        for nb_docs in nb_docs_space:

            # Update metrics object.
            metrics["nb_docs"] = nb_docs
            metrics["nb_tables"] = nb_tables
            metrics["nb_bits"] = nb_bits

            # Check if all of the metrics have been computed..
            skip = True
            for k1 in k1_space:
                metrics["k1"] = k1
                skip = skip and os.path.exists(get_metrics_path(metrics))

            if skip:
                nb_tests_done += len(k1_space)
                continue

            # Incremental indexing.
            metrics["index_times"] = aknn_index(
                docs_path=docs_path,
                es_hosts=es_hosts,
                es_index=vecs_index,
                es_type=vecs_type,
                aknn_uri="%s/%s/%s" % (model_index, model_type, model_id),
                nb_batch=metrics["nb_batch"],
                nb_total_max=nb_docs,
                skip_existing=True)

            print("Collecting %d ids and vectors for exact KNN" % (nb_docs))
            ids, vecs = [], np.zeros((nb_docs, nb_dimensions), dtype="float32")
            for doc in iter_es_docs(
                    es_host=next(es_hosts_c),
                    es_index=vecs_index,
                    es_type=vecs_type,
                    query={"_source": ["_aknn_vector"]}):
                ids.append(doc["_id"])
                vecs[len(ids) - 1] = np.array(doc["_source"]["_aknn_vector"])

            ii = random.sample(range(nb_docs), nb_eval)
            ids_sample = [ids[i] for i in ii]
            vecs_sample = vecs[ii]

            print("Running exact KNN")
            model = NearestNeighbors(
                k2 + 1, metric="euclidean", algorithm="brute")
            model.fit(vecs)
            nbrs_exact = model.kneighbors(vecs_sample, return_distance=False)

            # Execute searches for each value of k1.
            for k1 in k1_space:

                # Update metrics object.
                metrics["k1"] = k1

                # Run search to get times and nearest neighbors.
                metrics["search_times"], search_hits = aknn_search(
                    es_hosts=es_hosts,
                    es_index=vecs_index,
                    es_type=vecs_type,
                    es_ids=ids_sample,
                    k1=k1,
                    k2=k2 + 1,
                    nb_requests=nb_eval,
                    nb_workers=1)

                # Compute recall for each set of hits.
                metrics["search_recalls"] = []
                for i in range(len(ids_sample)):
                    a = [ids[i] for i in nbrs_exact[i]]
                    b = [h["_id"] for h in search_hits[i]]
                    assert a[0] == b[0] == ids_sample[i], \
                        "Zeroth values not matching: %s, %s, %s" % (
                        a[0], b[0], ids_sample[i])
                    metrics["search_recalls"].append(
                        len(np.intersect1d(a[1:], b[1:])) / k2)

                # Write results to file.
                s1 = "Saving metrics to %s" % get_metrics_path(metrics)
                print("%s\n%s" % ("-" * len(s1), s1))
                with open(get_metrics_path(metrics), "w") as fp:
                    json.dump(metrics, fp)

                nb_tests_done += 1
                s2 = "Completed %d of %d tests in %d minutes" % (
                    nb_tests_done, nb_tests_total, (time() - T0) / 60)
                print("%s\n%s" % (s2, "-" * len(s1)))


if __name__ == "__main__":

    ap = ArgumentParser(description="Elasticsearch-Aknn CLI")
    ap.add_argument("--es_hosts", default="http://localhost:9200",
                    help="comma-separated list of elasticsearch endpoints")

    sp_base = ap.add_subparsers(
        title='actions', description='Choose an action')

    sp = sp_base.add_parser("create")
    sp.set_defaults(which="create")
    sp.add_argument("docs_path", type=str)
    sp.add_argument("--sample_prob", type=float, default=0.3)
    sp.add_argument("--sample_seed", type=int, default=865)
    sp.add_argument("--es_index", type=str, default="aknn_models")
    sp.add_argument("--es_type", type=str, default="aknn_model")
    sp.add_argument("--es_id", type=str, required=True)
    sp.add_argument("--description", type=str, required=True)
    sp.add_argument("--nb_dimensions", type=int, required=True)
    sp.add_argument("--nb_tables", type=int, default=50)
    sp.add_argument("--nb_bits", type=int, default=16)

    sp = sp_base.add_parser("index")
    sp.set_defaults(which="index")
    sp.add_argument("docs_path", type=str)
    sp.add_argument("--aknn_uri", type=str, required=True)
    sp.add_argument("--es_index", type=str, required=True)
    sp.add_argument("--es_type", type=str, required=True)
    sp.add_argument("--nb_batch", type=int, default=5000)
    sp.add_argument("--nb_total_max", type=int, default=100000)
    sp.add_argument("--skip_existing", action="store_true")

    sp = sp_base.add_parser("benchmark")
    sp.set_defaults(which="benchmark")
    sp.add_argument("docs_path", type=str)
    sp.add_argument("--metrics_dir", type=str, default="metrics")
    sp.add_argument("--nb_dimensions", type=int, default=25)
    sp.add_argument("--nb_eval", type=int, default=500)
    sp.add_argument("--nb_batch", type=int, default=25000)
    sp.add_argument("--k2", type=int, default=10)
    sp.add_argument("--confirm_destructive", action="store_true")

    args = vars(ap.parse_args())
    action = args["which"]
    del args["which"]

    if action == "create":
        aknn_create(**args)

    if action == "benchmark":
        aknn_benchmark(**args)
