from elasticsearch import Elasticsearch, helpers
import json
import pdb
import requests
import numpy as np
import os
import re

ES_URL = "http://localhost:9200/_aknn_index"
RAW_GLOVE_PATH = os.path.expanduser("~") + "/Downloads/glove.840B.300d.txt"
BATCH_SIZE = 5000

INDEX = 'glove_word_vectors'
TYPE = 'word_vector'
ANN_URI = 'aknn_models/aknn_model/glove_840B_300D'


es = Elasticsearch()

body = json.loads("""{
  "mappings": {
    "%s": {
      "properties": {
        "description": {
          "type": "text",
          "index": false
        },
        "vector": {
          "type": "float",
          "index": false
        }
      }
    }
  }
}""" % TYPE)

es.indices.delete(index=INDEX, ignore=[400, 404])
es.indices.create(index=INDEX, body=body)

data = dict(_index=INDEX, _type=TYPE, _ann_uri=ANN_URI, docs=[])

regex = re.compile('[^a-zA-Z]')

for line in open(RAW_GLOVE_PATH):
    tkns = line.split(" ")
    word, vector = tkns[0], list(map(float, tkns[1:]))
    word = regex.sub('', word)
    if len(word) == 0:
        continue

    data['docs'].append(dict(_id=word, _source=dict(
        description=word, _aknn_vector=vector)))

    if len(data['docs']) == BATCH_SIZE:
        response = requests.post(ES_URL, json=data)
        print(response.json())
        data['docs'] = []
