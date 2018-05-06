"""
Compute features from images.
Reads pointers to images in S3 from a Kafka Topic.
Computes their features (labels, continuous feature vector).
Uploads features to S3.
Publishes pointer to features to Kafka.
"""

from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, wait
from kafka import KafkaConsumer, KafkaProducer
from io import BytesIO
from multiprocessing import Pool, cpu_count
from imageio import imread
from lycon import resize
from pprint import pformat
from sys import stderr
from time import time
import boto3
import gzip
import json
import numpy as np
import pdb

from keras.models import Model
from keras.applications import MobileNet
from keras.applications.imagenet_utils import preprocess_input, decode_predictions


def S3Pointer(id, s3_bucket, s3_key):
    return dict(id=id, s3_bucket=s3_bucket, s3_key=s3_key)


def FeaturesObject(id, img_pointer, imagenet_labels, feature_vector):
    if not isinstance(feature_vector, list):
        feature_vector = list(map(float, feature_vector))
    return dict(id=id, img_pointer=img_pointer,
                imagenet_labels=imagenet_labels,
                feature_vector=feature_vector)


class Convnet(object):

    def __init__(self):
        self.preprocess_mode = 'tf'
        model = MobileNet(weights='imagenet')
        self.model = Model(
            model.input, [model.output, model.get_layer('conv_preds').output])

    def get_labels_and_vecs(self, imgs_iter):

        imgs = np.array(imgs_iter)
        imgs = preprocess_input(imgs.astype(np.float32),
                                mode=self.preprocess_mode)

        clsf, vecs = self.model.predict(imgs)
        labels = [' '.join([y[1].lower() for y in x])
                  for x in decode_predictions(clsf, top=10)]
        vecs = np.squeeze(vecs)

        return labels, vecs


def _get_img_bytes_from_s3(args):
    bucket, key, s3client = args
    obj = s3client.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read()


def _preprocess_img(img_bytes):

    # Read from bytes to numpy array.
    try:
        img = imread(BytesIO(img_bytes))
        assert isinstance(img, np.ndarray)
    except (ValueError, AssertionError) as ex:
        print("Error reading image, returning zeros:", ex, file=stderr)
        return np.zeros((224, 224, 3), dtype=np.uint8)

    # Extremely fast resize using lycon.
    img = resize(img, 224, 224, interpolation=0)

    # Regular image: return.
    if img.shape[-1] == 3:
        return img

    # Grayscale image: repeat up to 3 channels.
    elif len(img.shape) == 2:
        return np.repeat(img[:, :, np.newaxis], 3, -1)

    # Other image: repeat first channel 3 times.
    return np.repeat(img[:, :, :1], 3, -1)


def _str_to_gzipped_bytes(s):
    b = s.encode()
    g = gzip.compress(b)
    return BytesIO(g)


if __name__ == "__main__":

    ap = ArgumentParser(description="See script header")
    ap.add_argument("--kafka_sub_topic",
                    help="Name of topic from which images are consumed",
                    default="aknn-demo.image-pointers")
    ap.add_argument("--kafka_pub_topic",
                    help="Name of topic to which feature vectors get produced",
                    default="aknn-demo.feature-pointers")
    ap.add_argument("--s3_pub_bucket",
                    help="Name of bucket to which feature vectors get saved",
                    default="klibisz-aknn-demo")
    ap.add_argument("--kafka_sub_offset",
                    help="Where to start reading from topic",
                    default="earliest", choices=["earliest", "latest"])
    ap.add_argument("--kafka_servers",
                    help="Bootstrap servers for Kafka",
                    default="ip-172-31-19-114.ec2.internal:9092")
    ap.add_argument("--kafka_group",
                    help="Group ID for Kafka consumer",
                    default="aknn-demo.compute-image-features")

    args = vars(ap.parse_args())
    print("Parsed command-line arguments:\n%s" % pformat(args))

    consumer = KafkaConsumer(
        args["kafka_sub_topic"],
        bootstrap_servers=args["kafka_servers"],
        group_id=args["kafka_group"],
        auto_offset_reset=args["kafka_sub_offset"],
        key_deserializer=lambda k: k.decode(),
        value_deserializer=lambda v: json.loads(v.decode())
    )
    producer = KafkaProducer(
        bootstrap_servers=args["kafka_servers"],
        compression_type='gzip',
        key_serializer=str.encode,
        value_serializer=str.encode)

    s3client = boto3.client('s3')

    convnet = Convnet()

    pool = Pool(cpu_count())
    tpex = ThreadPoolExecutor(max_workers=min(cpu_count() * 4, 20))

    print("Consuming from %s..." % args["kafka_sub_topic"])

    for msg in consumer:

        print("-" * 80)
        print("Received batch %s with %d images" % (msg.key, len(msg.value)))
        T0 = time()

        # Download images from S3 into memory using thread parallelism.
        t0 = time()
        try:
            f = lambda p: (p['s3_bucket'], p['s3_key'], s3client)
            data = map(f, msg.value)
            imgs_bytes = list(tpex.map(_get_img_bytes_from_s3, data))
        except Exception as ex:
            print("Error downloading images:", ex, file=stderr)
            continue
        print("Download images from S3: %.2lf seconds" % (time() - t0))

        # Preprocess the raw bytes using process parallelism.
        t0 = time()
        try:
            imgs_iter = pool.map(_preprocess_img, imgs_bytes)
        except Exception as ex:
            print("Error preprocessing images:", ex, file=stderr)
            continue
        print("Preprocess images: %.2lf seconds" % (time() - t0))

        # Compute image labels and feature vectors.
        t0 = time()
        try:
            labels, vecs = convnet.get_labels_and_vecs(imgs_iter)
        except Exception as ex:
            print("Error computing features:", ex, file=stderr)
            continue
        print("Compute features: %.2lf seconds" % (time() - t0))
        print("Vectors shape, mean, std = %s, %.5lf, %.5lf" % (
            vecs.shape, vecs.mean(), vecs.std()))

        t0 = time()
        s3_futures = []
        for img_pointer, label, vec in zip(msg.value, labels, vecs):

            # Create features object which will be uploaded to S3.
            features_object = FeaturesObject(
                id=img_pointer["id"], img_pointer=img_pointer,
                imagenet_labels=label, feature_vector=vec)

            # Create S3 Pointer which will be passed along in Kafka.
            features_pointer = S3Pointer(
                id=img_pointer["id"], s3_bucket=args["s3_pub_bucket"],
                s3_key="img-features-%s.json.gz" % img_pointer["id"])

            # Upload features object to S3 by submitting to the thread pool.
            try:
                s3_args = dict(
                    Body=_str_to_gzipped_bytes(json.dumps(features_object)),
                    Bucket=features_pointer['s3_bucket'],
                    Key=features_pointer['s3_key'])
                s3_futures.append(tpex.submit(s3client.put_object, **s3_args))
            except Exception as ex:
                print("Error uploading to S3:", ex, file=stderr)
                continue

            # Publish to Kafka.
            try:
                producer.send(args["kafka_pub_topic"],
                              key=features_pointer['id'],
                              value=json.dumps(features_pointer))
            except Exception as ex:
                print("Error publishing to Kafka:", ex, file=stderr)
                continue

        # Wait for all s3 requests to complete.
        try:
            wait(s3_futures, timeout=30)
        except Exception as ex:
            print("Error resolving upload futures:", ex, file=stderr)
            continue
        print("Upload features", time() - t0)

        print("Finished batch %s in %d seconds" % (msg.key, time() - T0))

    producer.flush()
