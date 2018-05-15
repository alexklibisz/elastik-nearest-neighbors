import json
import sys

assert len(sys.argv) > 1, "Usage: <script.py> path-to-unzipped-glove-vecs.txt"

for i, line in enumerate(open(sys.argv[1])):
    tkns = line.split(" ")
    word = tkns[0] 
    vector = list(map(lambda x: round(float(x), 5), tkns[1:]))
    doc = {
        "_id": "word_%d" % i,
        "_source": {
            "word": word,
            "_aknn_vector": vector
        }
    }
    print(json.dumps(doc))
