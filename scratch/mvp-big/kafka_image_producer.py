from kafka import KafkaProducer
from tqdm import tqdm
import base64
import sys
import os
import pdb
import random

KAFKA_SERVER = "ip-172-31-19-114.ec2.internal:9092"
KAFKA_TOPIC = "aknn-demo-twitter-images-base64"
KAFKA_TOPIC = "test-r1-p10"

if __name__ == "__main__":
	
	images_dir = sys.argv[1]
	N = int(sys.argv[2])

	producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)
	image_paths = os.listdir(images_dir)
	
	pbar = tqdm(random.sample(image_paths, N))
	for image_fname in pbar:
		with open("%s/%s" % (images_dir, image_fname), "rb") as fp:
			b64 = base64.b64encode(fp.read())
		producer.send(KAFKA_TOPIC, key=image_fname.encode(), value=b64)
		pbar.set_description(image_fname)

	producer.flush()


	
