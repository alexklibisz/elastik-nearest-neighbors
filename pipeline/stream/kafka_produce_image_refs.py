"""
Produce images references to a Kafka topic.
"""
from argparse import ArgumentParser
from kafka import KafkaProducer
from pprint import pprint
import json

if __name__ == "__main__":

    ap = ArgumentParser(description="See script header")
    ap.add_argument("index_file", help="Path to local index of S3 references with bucket and key columns")
    ap.add_argument("--kafka_pub_topic", help="Name of topic to produce to", default="aknn-demo.image-refs")
    ap.add_argument("--kafka_servers", help="Bootstrap servers for producer", 
        default="ip-172-31-19-114.ec2.internal:9092,ip-172-31-18-192.ec2.internal:9092,ip-172-31-20-205.ec2.internal:9092")
    ap.add_argument("-b", "--batch_size", type=int, default=1000, help="Number of images to produce at once")
    args = vars(ap.parse_args())
    
    producer = KafkaProducer(bootstrap_servers=args["kafka_servers"])
    fp = open(args["index_file"])
    batch = []
    
    for bucket, key in map(str.split, tqdm(fp)):    
        batch.append(json.dumps(dict(bucket=bucket, key=key)))
        if len(batch) < args["batch_size"]:
            continue
        value = json.dumps(batch).encode()
        producer.send(args["kafka_pub_topic"], value=value)
        batch = []
