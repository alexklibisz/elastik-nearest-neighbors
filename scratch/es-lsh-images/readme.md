
## Introduction

1. Use Imagenet test set downloaded from Kaggle Imagenet competition.
2. Use Xception architecture with Imagenet weights to product 2048-dimensional feature vectors.
3. Run an exact KNN with cosine distance. See results in `imagenet_knn_exact.ipynb`.
4. Run a very simple LSH on the vectors. Insert the hashes in ES as text documents. Run similarity search. See very promising results in `imagenet_es_lsh.ipynb`.

## Results

See the two notebooks for visual results.