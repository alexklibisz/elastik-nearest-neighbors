from __future__ import print_function
from pprint import pprint
import json
import re
import requests
import pdb
import random
import sys

ES_BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:9200"
ES_POST_URL = "%s/_aknn_index" % ES_BASE_URL
RAW_GLOVE_PATH = "glove.840B.300d.txt"
BATCH_SIZE = 10000

data = {
    "_index": "glove_word_vectors",
    "_type": "word_vector",
    "_aknn_uri": "aknn_models/aknn_model/glove_840B_300D",
    "_aknn_docs": [
        # Populated below.
    ]
}


re_letters_only = re.compile("[^a-zA-Z]")

for line in open(RAW_GLOVE_PATH):
    tkns = line.split(" ")
    word, vector = tkns[0], list(map(float, tkns[1:]))
    word = re_letters_only.sub("", word)
    if len(word) == 0:
        continue

    data["_aknn_docs"].append({
        "_id": word,
        "_source": {
            "description": word,
            "_aknn_vector": vector
        }
    })

    if len(data["_aknn_docs"]) == BATCH_SIZE:

        print("Posting %d docs to Elasticsearch" % BATCH_SIZE)
        response = requests.post(ES_POST_URL, json=data)
        pprint(response.json())
        data["_aknn_docs"] = []

        print("Getting neighbors for word %s" % word)
        response = requests.get("%s/%s/%s/%s/_aknn_search?k2=3" % (
            ES_BASE_URL, data['_index'], data['_type'], word))
        print("%s -> %s" % (word, ", ".join([x["_id"] for x in response.json().get("hits").get("hits")])))

