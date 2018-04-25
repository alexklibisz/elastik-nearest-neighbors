# Reads S3 image keys from a text file and produces them to a Kafka topic.
# from kafka import KafkaProducer
from confluent_kafka import Producer
from time import time, sleep
import random

if __name__ == "__main__":

    S3_KEYS_PATH = 's3_keys.txt'
    KAFKA_SERVER = 'localhost:9092'
    OUTPUT_TOPIC = 'image-s3-keys'

    producer = Producer({"bootstrap.servers": KAFKA_SERVER})

    with open(S3_KEYS_PATH) as fp:
        s3_keys = list(map(str.strip, fp))

    while True:
        s3_key = random.choice(s3_keys)
        producer.produce(OUTPUT_TOPIC, key=s3_key, value=s3_key)
        print('Produced key %s' % s3_key)
        sleep(0.1)

    producer.flush()
