# Kafka consumer with following responsibilities:
# - Read LSH vector from topic.
# - Insert LSH vector to Elasticsearch.
from elasticsearch import Elasticsearch, helpers
from confluent_kafka import Consumer, KafkaError, Producer
from time import time
import numpy as np
import pdb
import os


def vec_to_text(vec):
    tokens = []
    for i, b in enumerate(vec):
        tokens.append("%d_%d" % (i, b))
    return " ".join(tokens)


if __name__ == "__main__":

    K_SERVER = "localhost:9092"
    K_SUB_TOPIC = "image-hash-vectors"

    settings = {
        'bootstrap.servers': K_SERVER,
        'group.id': 'TODO',
        'client.id': 'client-%d' % time(),
        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'default.topic.config': {
            'auto.offset.reset': 'smallest'
        }
    }

    consumer = Consumer(settings)
    consumer.subscribe([K_SUB_TOPIC])

    es = Elasticsearch()
    actions = []

    while True:

        # TODO: is it really best practice to poll like this?
        msg = consumer.poll(0.1)
        if msg is None:
            continue

        if msg.error():
            print('Error: %s' % msg.error().str())
            continue

        image_key = msg.key().decode()
        hsh = np.fromstring(msg.value(), dtype=np.uint8)

        actions.append({
            "_index": "imagenet_images",
            "_type": "image",
            "_id": image_key,
            "_source": {
                "image_key": image_key,
                "text": vec_to_text(hsh)
            }
        })

        # TODO: actually do a bulk insertion...
        helpers.bulk(es, actions)
        actions = []

        print('%s %s %.3lf' % (image_key, str(hsh.shape), hsh.mean()))

    # TODO: use atexit to make sure it flushes if the script fails.
    producer.flush()
