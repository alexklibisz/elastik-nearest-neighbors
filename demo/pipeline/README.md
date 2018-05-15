
Workers and scripts for the Twitter Image Similarity search demo.

## Overview

Below is a terse overview of the functionality for each program in the pipeline.
See individual programs for more detail.

1. `ingest_twitter_images.py` ingests tweets from Twitter's streaming API and 
saves posted images locally and on S3. This ingests between 500K and 700K images
per day. `twitter-credentials.template.json` should be updated with your Twitter
API credentials to run this program.
2. `stream_produce_image_pointers.py` produces pointers to images to a Kafka topic.
A pointer is simply the S3 bucket and key where the image file is stored.
3. `stream_compute_image_features.py` consumes images pointers and computes 
a floating-point feature vector for each image. It stores the feature vectors
on S3 and publishes a pointer to the features to a Kafka topic. This program was
designed such that many instances can be run in parallel to speed up computation. 
As long as they are all in the same Kafka consumer group, each one will get 
independent chunks of the processing load.
4. `batch_es_aknn_create.py` creates an LSH model in Elasticsearch via the Elasticsearch-Aknn plugin.
5. `batch_es_aknn_index.py` indexes feature vectors in Elasticsearch via the
Elasticsearch-Aknn plugin.

## Usage

Install dependencies: `pip3 install -r requirements.txt`

All Python programs implement an argparse CLI, so you can run `python <name>.py --help` to see the exact parameters.

Most of the programs require a Kafka cluster and an Elasticsearch cluster. 
Instructions to set them up is beyond the scope of this brief documentation, 
however the `ec2_es_setup.sh` script should be helpful for Elasticsearch.