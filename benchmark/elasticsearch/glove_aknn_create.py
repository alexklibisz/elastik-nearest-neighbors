from __future__ import print_function
import json
import random
import requests
import sys

ES_BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:9200"
ES_POST_URL = "%s/_aknn_create" % ES_BASE_URL

MODEL_DOC = {

    "_index": "aknn_models",
    "_type": "aknn_model",
    "_id": "glove_840B_300D",

    "_source": {
        "_aknn_description": "AKNN model for Glove Common Crawl 840B word vectors (http://nlp.stanford.edu/data/glove.840B.300d.zip).",
        "_aknn_nb_dimensions": 300,
        "_aknn_nb_tables": 32,
        "_aknn_bits_per_table": 16
    },

    "_aknn_vector_sample": [
        # Populated below.
    ]
}

GLOVE_RAW_PATH = "./glove.840B.300d.txt"

print("Reading Glove vectors from disk")
with open(GLOVE_RAW_PATH) as fp:
    glove_lines = fp.readlines()

sample_size = MODEL_DOC['_source']['_aknn_nb_tables']
sample_size *= MODEL_DOC['_source']['_aknn_bits_per_table']
sample_size *= 2

print("Taking sample of size %d" % sample_size)
for line in random.sample(glove_lines, sample_size):
    vec = list(map(float, line.split(" ")[1:]))
    MODEL_DOC['_aknn_vector_sample'].append(vec)

print("Posting to Elasticsearch")
response = requests.post(ES_POST_URL, json=MODEL_DOC)
