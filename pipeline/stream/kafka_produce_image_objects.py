from argparse import ArgumentParser
from kafka import KafkaProducer
from time import time
from tqdm import tqdm
import boto3
import json
import pdb

if __name__ == "__main__":
    
    ap = ArgumentParser(description="See script")
    ap.add_argument("--bucket", help="S3 bucket name", 
                    default="klibisz-twitter-stream")
    ap.add_argument("--kafka_pub_topic", 
                    help="Topic to which image events get published", 
                    default="aknn-demo.image-objects")
    ap.add_argument("--kafka_server", 
                    help="Bootstrap server for producer", 
                    default="ip-172-31-19-114.ec2.internal:9092")
    ap.add_argument("-b", "--batch_size", type=int, default=1000,
                    help="Size of batches produced")
    
    args = vars(ap.parse_args())

    bucket = boto3.resource("s3").Bucket(args["bucket"])
    producer = KafkaProducer(
        bootstrap_servers=args["kafka_server"],
        compression_type="gzip")

    t0 = time()
    nb_produced = 0
    batch = []
    for obj in bucket.objects.all():

        # TODO: this was a bad design in the ingestion.
        # There should be a way to distinguish statues
        # from images without ever reading in the statuses.
        if obj.key.endswith(".json.gz"):
            continue

        batch.append(dict(bucket=obj.bucket_name, key=obj.key))

        if len(batch) < args["batch_size"]:
            continue
    
        value = json.dumps(batch).encode()
        producer.send(args["kafka_pub_topic"], value)
        nb_produced += len(batch)
        batch = []

        print("%d produced - %d / second" % (
            nb_produced, nb_produced / (time() - t0)))
