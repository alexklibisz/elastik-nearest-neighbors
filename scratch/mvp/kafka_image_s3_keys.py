# Reads S3 image keys from a text file and produces them to a Kafka topic.
from confluent_kafka import Producer
from time import sleep

if __name__ == "__main__":

    S3_KEYS_PATH = 's3_keys_test.txt'
    KAFKA_SERVER = 'localhost:9092'
    OUTPUT_TOPIC = 'image-s3-keys'

    producer = Producer({"bootstrap.servers": KAFKA_SERVER})

    for i, s3_key in enumerate(map(str.strip, open(S3_KEYS_PATH))):
        producer.produce(OUTPUT_TOPIC, key=s3_key, value=s3_key)
        print('%d, key %s' % (i, s3_key))
        sleep(0.0001)

    producer.flush()
