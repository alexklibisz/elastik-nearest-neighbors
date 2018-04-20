"""Hash each word's vector and insert it to elastic search."""

import numpy as np
import math
import pdb


def get_signature(data, planes):
    sig = np.zeros(len(planes), dtype=np.uint8)
    for i, p in enumerate(planes):
        sig[i] = int(np.dot(data, p) >= 0)
    return sig


def get_doc(sig):
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

    for vec in vecs:

        sig = get_signature(vec, lsh_planes)
        doc = get_doc(sig)
        print(np.sum(sig))
