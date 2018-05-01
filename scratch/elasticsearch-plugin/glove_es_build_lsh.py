from elasticsearch import Elasticsearch, helpers
import json
import pdb
import requests
import numpy as np

ES_URL = "http://localhost:9200/_build_lsh"

L, H, D = 10, 8, 5

vector_sample = [
    list(map(lambda x: float(round(x, 8)), point))
    for point in np.random.normal(0, 1, size=(2 * L * H, D))
]

vector_sample = np.random.normal(0, 1, size=(2 * L * H, D)).astype(np.float16)
vector_sample_csv = ""
for vector in vector_sample:
    vector_sample_csv += ",".join(map(lambda x: "%.8lf" % x, vector)) + "\n"

data = {
    "_index": "lsh_models",
    "_type": "lsh_model",
    "_id": "glove_840B_300D",
    "description": "LSH for Glove Common Crawl 840B (Glove.840B.300d.zip)",

    "nb_tables": L,
    "nb_bits_per_table": H,
    "nb_dimensions": D,

    "vector_sample_csv": vector_sample_csv
}

requests.post(ES_URL, json=data)
