from elasticsearch import Elasticsearch, helpers
from sklearn.neighbors import NearestNeighbors
from time import time
from tqdm import tqdm
import json
import os
import numpy as np
import pdb

from lsh_model import LSHModel

N = 400000
L = 32
H = 12
RAW_GLOVE_PATH = os.path.expanduser("~") + "/Downloads/glove.6B.50d.txt"
GLOVE_VOC_PATH = "glove_artifacts/glove_vocab.txt"
GLOVE_VEC_PATH = "glove_artifacts/glove_vectors.npy"
GLOVE_KNN_PATH = "glove_artifacts/glove_knn.txt"
GLOVE_TEST_WORDS = ["obama", "quantum", "neural", "olympics", "san", "york",
                    "nuclear", "data", "pink", "monday"]
LSH_HASHES_PATH = "glove_artifacts/glove_lsh_hashes.txt"

# Convert the raw glove data into a vocab file and a numpy array file.
if not (os.path.exists(GLOVE_VOC_PATH) and os.path.exists(GLOVE_VEC_PATH)):

    words, vecs = [], np.zeros((N, 50))

    with open(RAW_GLOVE_PATH) as fp:
        for i, line in tqdm(enumerate(fp), desc="Processing raw GLOVE data"):
            if i == N:
                break
            tkns = line.split()
            words.append(tkns[0])
            vecs[i] = np.array(list(map(float, tkns[1:])))

    with open(GLOVE_VOC_PATH, "w") as fp:
        fp.write("\n".join(words))

    np.save(GLOVE_VEC_PATH, vecs.astype(np.float32))


# Compute the real nearest neighbors for a set of test words.
if not (os.path.exists(GLOVE_KNN_PATH)):

    with open(GLOVE_VOC_PATH) as fp:
        words = list(map(str.strip, fp))
        word2idx = {w: i for i, w in enumerate(words)}

    vecs = np.load(GLOVE_VEC_PATH)
    knn = NearestNeighbors(n_neighbors=5, algorithm='brute', metric='euclidean')
    knn.fit(vecs)

    test_ii = list(map(word2idx.get, GLOVE_TEST_WORDS))
    nbrs = knn.kneighbors(vecs[test_ii], return_distance=False)

    with open(GLOVE_KNN_PATH, "w") as fp:
        for word, nbrs_ in zip(GLOVE_TEST_WORDS, nbrs):
            fp.write("%s %s\n" % (word, " ".join([words[i] for i in nbrs_])))

# Fit LSH models and compute the hash from each model on each word vector.
if not os.path.exists(LSH_HASHES_PATH):

    with open(GLOVE_VOC_PATH) as fp:
        words = list(map(str.strip, fp))
        word2idx = {w: i for i, w in enumerate(words)}

    vecs = np.load(GLOVE_VEC_PATH)

    lsh_models = [LSHModel(seed=i, H=H).fit(vecs) for i in range(L)]

    for i, lsh_model in enumerate(lsh_models):
        print("model %d mean per bucket = %.3lf" %
              (i, lsh_model.get_hash(vecs).sum(axis=-1).mean()))

    lines = []

    for word, vec in tqdm(zip(words, vecs), desc="Computing LSH hashes for each vector"):
        lines.append(word)
        for lsh_model in lsh_models:
            hash_arr = lsh_model.get_hash(vec)
            hash_str = ''.join(map(str, hash_arr))
            hash_int = int(hash_str, 2)
            lines[-1] = "%s %d" % (lines[-1], hash_int)

    with open(LSH_HASHES_PATH, "w") as fp:
        fp.write("\n".join(lines))


# Finally, insert the documents to elasticsearch.

es = Elasticsearch()
actions = []

hashes = ",".join(['"%d": {"type": "integer"}' % i for i in range(L)])
body = json.loads("""{
  "mappings": {
    "hashed_vector": {
      "properties": {
        "description": {
          "type": "text",
          "index": false
        },
        "hashes": {
            "properties": { %s }
        }
      }
    }
  }
}""" % hashes)

es.indices.delete(index="glove_hashed_vectors", ignore=[400, 404])
es.indices.create(index="glove_hashed_vectors", body=body)

with open(LSH_HASHES_PATH) as fp:

    for i, line in enumerate(map(str.strip, fp)):
        tkns = line.split()
        word, hashes = tkns[0], tkns[1:]

        actions.append({
            "_index": "glove_hashed_vectors",
            "_type": "hashed_vector",
            "_id": word,
            "_source": {
                "description": word,
                "hashes": {str(j): int(h) for j, h in enumerate(hashes)}
            }
        })

        if len(actions) == 10000:
            helpers.bulk(es, actions)
            print("Inserted %d of %d" % (i, N))
            actions = []
