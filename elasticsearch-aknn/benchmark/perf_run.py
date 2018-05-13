from itertools import product
import json
import os
import pdb

from aknn import aknn_create, aknn_index, aknn_latency_recall, aknn_delete

es_hosts = ["http://localhost:9200"]
es_index_model = "aknn"
es_type_model = "model"
es_index_vec = "glove_vecs"
es_type_vec = "vec"
docs_path = "glove.6B.50d.docs.txt"
nb_dimensions = 50

configs = [

    (10000, 50, 7, [50, 100]),
    (10000, 50, 8, [50, 100]),
    (10000, 50, 9, [50, 100]),

    (10000, 75, 7, [50, 100]),
    (10000, 75, 8, [50, 100]),

    (10000, 100, 7, [50, 100]),
    (10000, 100, 8, [50, 100])

]

for i, (nb_docs, nb_tables, nb_bits, k1_all) in enumerate(configs):

    print("-" * 80)
    print("Test %d of %d: docs=%d, tables=%d, bits=%d, k1_all=%s" %
          (i + 1, len(configs), nb_docs, nb_tables, nb_bits, str(k1_all)))
    print("-" * 80)

    k1_to_result_path = {k1: "metrics/latrec_%d_%d_%d_%d.json" % (
        nb_docs, nb_tables, nb_bits, k1) for k1 in k1_all}

    if sum(map(os.path.exists, k1_to_result_path.values())) == len(k1_all):
        print("Already computed")
        continue

    es_id_model = "glove_%d_%d_%d" % (nb_docs, nb_tables, nb_bits)

    # Delete existing indexes.
    aknn_delete(es_hosts[0], es_index_model)
    aknn_delete(es_hosts[0], es_index_vec)

    # Create the model for this configuration.
    aknn_create(
        docs_path,
        es_hosts=es_hosts,
        es_index=es_index_model,
        es_type=es_type_model,
        es_id=es_id_model,
        description="Glove test model",
        nb_dimensions=nb_dimensions,
        nb_tables=nb_tables,
        nb_bits=nb_bits,
        sample_prob=0.3,
        sample_seed=1)

    # Index documents.
    aknn_index(
        docs_path,
        es_hosts=es_hosts,
        es_index=es_index_vec,
        es_type=es_type_vec,
        aknn_uri="%s/%s/%s" % (es_index_model, es_type_model, es_id_model),
        nb_batch=min(20000, nb_docs),
        nb_total_max=nb_docs)

    # results = [{'k1': ..., 'latencies': [...], 'recalls': [...]}]
    results = aknn_latency_recall(
        es_hosts=es_hosts,
        es_index=es_index_vec,
        es_type=es_type_vec,
        nb_sampled=500,
        k1_all=k1_all,
        k2=10,
        sample_seed=1)

    # Hydrate the results with test settings and save to disk.
    for i in range(len(results)):
        results[i]["nb_tables"] = nb_tables
        results[i]["nb_bits"] = nb_bits
        results[i]["nb_docs"] = nb_docs

        result_path = k1_to_result_path[results[i]["k1"]]
        with open(result_path, "w") as fp:
            json.dump(results[i], fp)
