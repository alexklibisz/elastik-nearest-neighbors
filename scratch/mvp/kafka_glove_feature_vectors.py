# Kafka consumer with following responsibilities:
# - Read S3 key from Kafka topic.
# - Compute feature vector via pre-trained convnet.
# - Publish feature vector to topic.
# See below link for description of configuration:
# https://www.confluent.io/blog/introduction-to-apache-kafka-for-python-programmers/

from confluent_kafka import Consumer, KafkaError, Producer
from time import time
import boto3
import numpy as np
import pdb

if __name__ == "__main__":

    K_SERVER = "localhost:9092"
    K_PUB_TOPIC = "glove-feature-vectors"

    producer = Producer({"bootstrap.servers": K_SERVER})

    glove_dir = '/home/alex/dev/approximate-vector-search/scratch/es-lsh-glove'
    glove_keys = [l.strip() for l in open('%s/glove_vocab.txt' % glove_dir)]
    glove_vecs = np.load('%s/glove_vecs.npy' % glove_dir).astype(np.float32)

    glove_keys = glove_keys[:90000]
    glove_vecs = glove_vecs[:90000]

    for i, (key, vec) in enumerate(zip(glove_keys, glove_vecs)):
        producer.produce(K_PUB_TOPIC, key=key, value=vec.tostring())
        print(i, key, vec.mean())

    producer.flush()
