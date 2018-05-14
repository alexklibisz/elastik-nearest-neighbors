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

The repository is structured as follows:

- `demo`: code for the Twitter Image Similarity search pipeline and web-application.
- `elasticsearch-aknn`: EsAknn implementation and benchmarks.
- `scratch`: several smaller sub-projects implemented while prototyping.

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

#### Speed

EsAknn's speed can be characterized:

1. Search time scales sub-linearly with the size of the corpus.
2. Create a new LSH model: < 1 minute.
3. Index new vectors: hundreds to low thousands per second.
4. Search for a vector's neighbors: < 500 milliseconds.

Beyond that, speed depends on:

1. The dimensionality of the vectors.
2. The number of tables (a.k.a. hash functions or trees) in the LSH model.
3. The number of bits in the LSH model's hashes.
4. The number of approximate neighbors retrieved, `k1`. 
5. The number of exact neighbors returned, `k2`.

In the image similarity search engine, you can see that searches against an
index of 6.7 million 1000-dimensional vectors rarely exceed 200 milliseconds.

#### Recall

Recall is defined as the proportion of true nearest neighbors returned for
a search. Similar to speed, recall depends on the data and LSH configuration.
Increasing `k1` is typically the best way to increase recall, but the number
of tables and bits also play a subtle but important role.

I am working on a more comprehensive benchmark for both speed and recall and
will update this section when it's ready.

## Image Processing Pipeline

### Implementation

<img src="demo/architecture.png" height=300 width="auto"/>

The image processing pipeline consists of the following components:

1. Python program ingests images from the Twitter public stream and stores on S3.
2. Python program publishes batches of references to images stored in S3 to a 
Kafka topic.
3. Python program consumes batches of image references, computes feature 
vectors from the images, stores them on S3, publishes references to Kafka. 
I use the `conv_pred` layer from 
[Keras pre-trained MobileNet](https://keras.io/applications/#mobilenet) 
to compute the 1000-dimensional feature vectors. 
4. Python program consumes image features from Kafka/S3 and indexes them in
Elasticsearch via EsAknn.

### Performance

The image feature extraction is the main bottleneck in this pipeline. It's 
embarrassingly parallel but still requires thoughtful optimization. In the end
I was able to compute:

1. 40 images / node / second with EC2 P2.xlarge (K80 GPU, $0.3/hr spot instance).
2. 33 images / node / second on EC2 C5.9xlaarge (36-core CPU, $0.6/hr spot instance).

My first-pass plateaued at about 2 images / node / second. I was able to improve
throughput with the following optimization:

1. Produce image references to Kafka instead of full images. This allows
many workers to download the images in parallel from S3. If you send the full
images through Kafka, it quickly becomes a bottleneck.
2. Workers use thread pools to download images in parallel from S3.
3. Workers use process pools to crop and resize images for use with Keras.
4. Workers use the [Lycon library](https://github.com/ethereon/lycon) for fast image resizing.
3. Workers compute feature vectors on large batches of images instead of single
images. This is a standard optimization with any deep neural network.