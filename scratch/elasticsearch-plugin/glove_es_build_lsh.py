from elasticsearch import Elasticsearch, helpers
import json
import pdb
import requests
import numpy as np

D = 300  # Dimension of each vector.
L = 32   # Number of LSH models.
H = 32   # Number of buckets in each LSH model.

ES_URL = "http://localhost:9200/_build_lsh"
GLOVE_VEC_PATH = "glove_artifacts/glove_vectors.npy"

np.random.seed(1)
vecs = np.load(GLOVE_VEC_PATH)
sample_ii = np.random.permutation(len(vecs))[:2 * L * H]

vector_sample_csv = ""
for vec in vecs[sample_ii]:
    vector_sample_csv += ",".join(map(lambda x: "%.8lf" % x, vec)) + "\n"

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

response = requests.post(ES_URL, json=data)
print(response.json())
