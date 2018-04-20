"""Hash each word's vector and insert it to elastic search."""
from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm
import numpy as np
import math
import pdb


def get_signature(data, planes):
    sig = np.zeros(len(planes), dtype=np.uint8)
    for i, p in enumerate(planes):
        sig[i] = int(np.dot(data, p) >= 0)
    return sig


def signature_to_text(sig):
    d = ""
    for i, v in enumerate(sig):
        d += "%d_%d " % (i, v)
    return d


if __name__ == '__main__':

    dim = 50       # Feature vector dimension.
    bits = 1024    # number of bits (planes) per signature

    lsh_planes = np.random.randn(bits, dim)

    vecs = np.load('glove_vecs.npy')
    vocab_w2i = {w.strip(): i for i, w in enumerate(open('glove_vocab.txt'))}
    vocab_i2w = {i: w for w, i in vocab_w2i.items()}

    es = Elasticsearch()
    actions = []

    for i, vec in tqdm(enumerate(vecs)):

        sgtr = get_signature(vec, lsh_planes)
        text = signature_to_text(sgtr)

        actions.append({
            "_index": "glove50",
            "_type": "word",
            "_id": i,
            "_source": {
                "word": vocab_i2w[i],
                "text": text
            }
        })

        if len(actions) == 10000:
            helpers.bulk(es, actions)
            actions = []
