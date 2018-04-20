"""Compute the exact KNN for the first 1000 Glove words"""
from sklearn.neighbors import NearestNeighbors
from tqdm import tqdm
import numpy as np
import pdb
import sys


if __name__ == "__main__":

    K = 100
    B = 500
    metric = sys.argv[1] if len(sys.argv) > 1 else 'cosine'

    vecs = np.load('glove_vecs.npy')
    vocab_w2i = {w.strip(): i for i, w in enumerate(open('glove_vocab.txt'))}
    vocab_i2w = {i: w for w, i in vocab_w2i.items()}

    N = np.zeros((len(vecs), K))

    knn = NearestNeighbors(n_neighbors=K, algorithm='brute', metric=metric)
    knn.fit(vecs)

    for i in tqdm(range(0, len(vecs), B)):
        query_words = [vocab_i2w[j] for j in range(i, i + B)]
        nbrs = knn.kneighbors(vecs[i:i + B], return_distance=False)
        for j, (w, ii) in enumerate(zip(query_words, nbrs)):
            # print('%s: %s' % (w, ' '.join([vocab_i2w[k] for k in ii])))
            N[i + j, :] = ii

    np.save('glove_neighbors_exact.npy', N)
