# Reads S3 image keys from a text file and produces them to a Kafka topic.
from kafka import KafkaProducer
from time import time, sleep
import random

if __name__ == "__main__":

	S3_KEYS_PATH = 's3_keys.txt'
	KAFKA_SERVER = 'localhost:9092'
	OUTPUT_TOPIC = 'image-s3-keys'

	producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

	with open(S3_KEYS_PATH) as fp:
		s3_keys = list(map(str.strip, fp))

	while True:
		s3_key = random.choice(s3_keys)
		producer.send(OUTPUT_TOPIC, s3_key.encode())
		producer.flush()
		print('Produced key %s' % s3_key)
		sleep(1)
