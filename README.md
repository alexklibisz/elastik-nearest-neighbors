# Approximate Vector Search

Insight Data Engineering Project, Boston, April - June 2018

***

<img src="demo/screencast.gif" height=320 width="auto"/>

## Overview

I implemented Elasticsearch-Aknn (EsAknn), an Elasticsearch plugin which adds 
functionality for approximate nearest neighbors search over dense,
floating-point vectors. 

To demonstrate the plugin, I used it to implement an image similarity search 
engine for a corpus of about 7 million Twitter images. The images were 
transformed into 1000-dimensional feature vectors using a convolutional neural 
network, and Elasticsearch-Aknn is used to store these vectors and search for 
nearest neighbors on an Elasticsearch cluster.

## Demo

Please see:

- [Screencast demo on Youtube.](https://www.youtube.com/watch?v=HqvbbwmY-0c)
- [Web-application.](http://ec2-18-204-52-148.compute-1.amazonaws.com:9999/twitter_images/twitter_image/demo) It will likely go away once the Insight program ends.
- [Presentation on Google Slides.](https://docs.google.com/presentation/d/1kkzwM-m5KvpfQFhqCepnR45_Pqz4poxPLtpbs3C8cXA/edit?usp=sharing)

## Elasticsearch-Aknn

### Usecase

EsAknn will be useful if your problem is roughly characterized:

1. Have a large corpus of feature vectors with dimensionality between ~50 and ~1000.
2. Need to search for similar vectors within your corpus using K-Nearest-Neighbors.
3. Need to scale horizontally to support many concurrent similarity searches.
4. Need to support a growing corpus with near-real-time insertions. I.e., 
when a new vector is created/ingested, it should be available for searching in 
less than 10 minutes.

### Features

EsAknn exposes the following high-level functionality as a set of HTTP endpoints:

1. Given a sample of vectors, create a locality-sensitive-hashing (LSH) model 
and store it as an Elasticsearch document.
2. Given a batch of new vectors, hash each vector using a pre-defined LSH model 
and store its raw and hashed values in an Elasticsearch document.
3. Given a vector in the index, search for and return its nearest neighbors.

### Implementation

The most important things to know about the implementation are:

1. EsAknn runs entirely in an existing Elasticsearch cluster/node.
2. Searches can run in parallel. New vectors can be indexed on multiple nodes 
in parallel using a round-robin strategy. Parallel indexing on a single node has
not been tested extensively.
3. EsAknn uses [Locality Sensitive Hashing](https://en.wikipedia.org/wiki/Locality-sensitive_hashing) 
to convert a floating-point vector into a discrete representation which can be 
efficiently  indexed and retrieved in Elasticsearch.
4. EsAknn stores the LSH models and the vectors as standard documents.
5. EsAknn uses a [Bool Query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html) 
to find `k1` approximate nearest neighbors based on discrete hashes. It then 
computes the exact distance to each of these approximate neighbors and returns 
the `k2` closest. For example, you might set `k1 = 1000` and `k2 = 10`.

### Performance

EsAknn's performance can be characterized:

1. Search time scales sub-linearly with the size of the corpus.
2. Create a new LSH model: < 1 minute.
3. Index new vectors: hundreds to low thousands per second.
4. Search for a vector's neighbors: < 500 milliseconds.

Beyond that, performance depends on:

1. The dimensionality of the vectors.
2. The number of tables (a.k.a. hash functions or trees) in the LSH model.
3. The number of bits in the LSH model's hashes.
4. The number of approximate neighbors retrieved, `k1`. 
5. The number of exact neighbors returned, `k2`.

In the image similarity search engine, you can see that searches against an
index of 6.7 million 1000-dimensional vectors rarely exceed 200 milliseconds.

I am working on a more cohesive benchmark and will update this section when
it's ready.

## Image Processing Pipeline

More details coming soon.
