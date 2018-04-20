"""Reads the Glove text file and creates a single numpy matrix
containing the vectors and a text file containing the words."""

import numpy as np

glove_path = "/home/alex/Downloads/glove.6B.50d.txt"

fp_glove = open(glove_path)
fp_words = open("glove_vocab.txt", "w")
words = []
vecs = np.zeros((400000, 50))

for i, line in enumerate(fp_glove):
    tkns = line.split()
    words.append(tkns[0])
    vecs[i, :] = np.array([float(x) for x in tkns[1:]])
    print(i, words[-1])


fp_words.write('\n'.join(words))
np.save('glove_vecs.npy', vecs.astype(np.float32))
