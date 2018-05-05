"""
Produce images from disk to a Kafka topic.
"""
from argparse import ArgumentParser
from kafka import KafkaProducer
from tqdm import tqdm
import os
import pdb

if __name__ == "__main__":

    ap = ArgumentParser(description="See script header")
    ap.add_argument("imgs_dir", 
                    help="Path to local directory of images")
    ap.add_argument("--kafka_pub_topic", 
                    help="Name of topic to which images are published", 
                    default="aknn-demo.images")
    ap.add_argument("--kafka_servers", 
                    help="Bootstrap servers for producer", 
                    default="ip-172-31-19-114.ec2.internal:9092,ip-172-31-18-192.ec2.internal:9092,ip-172-31-20-205.ec2.internal:9092")

    args = vars(ap.parse_args())
    
    producer = KafkaProducer(bootstrap_servers=args["kafka_servers"])

    for fobj in tqdm(os.scandir(args["imgs_dir"])):
        with open("%s/%s" % (args["imgs_dir"], fobj.name), "rb") as fp:
            producer.send(args["kafka_pub_topic"], key=fobj.name.encode(), value=fp.read())
    producer.flush()
 

