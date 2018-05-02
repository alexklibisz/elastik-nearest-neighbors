from elasticsearch import Elasticsearch, helpers
import json
import pdb
import requests
import numpy as np
import os
import re

ES_URL = "http://localhost:9200/_index_ann"
RAW_GLOVE_PATH = os.path.expanduser("~") + "/Downloads/glove.840B.300d.txt"
BATCH_SIZE = 5000

INDEX = 'glove_word_vectors'
TYPE = 'word_vector'
ANN_URI = 'ann_models/ann_model/glove_840B_300D'

data = dict(_index=INDEX, _type=TYPE, _ann_uri=ANN_URI, docs=[])

regex = re.compile('[^a-zA-Z]')

for line in open(RAW_GLOVE_PATH):
    tkns = line.split(" ")
    word, vector = tkns[0], list(map(float, tkns[1:]))
    word = regex.sub('', word)
    if len(word) == 0:
        continue

    data['docs'].append(dict(_id=word, _source=dict(vector=vector)))

    if len(data['docs']) == BATCH_SIZE:
        print([d['_id'] for d in data['docs']])
        response = requests.post(ES_URL, json=data)
        print(response.json())
        data['docs'] = []
