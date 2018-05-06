from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, wait
from kafka import KafkaConsumer, KafkaProducer
from io import BytesIO
from multiprocessing import Pool, cpu_count
from scipy.misc import imread, imsave
from lycon import resize
from time import time
import boto3
import gzip
import json
import numpy as np
import pdb

from keras.models import Model
from keras.applications import MobileNet
from keras.applications.imagenet_utils import preprocess_input, decode_predictions


class Convnet(object):

    def __init__(self):
        self.preprocess_mode = 'tf'
        model = MobileNet(weights='imagenet')
        self.model = Model(model.input, [model.output, model.get_layer('conv_preds').output])
    
    def get_labels_and_vecs(self, imgs_iter):

        imgs = np.array(imgs_iter)
        imgs = preprocess_input(imgs.astype(np.float32), mode=self.preprocess_mode)

        clsf, vecs = self.model.predict(imgs)
        labels = [' '.join([y[1].lower() for y in x]) for x in decode_predictions(clsf, top=10)]
        vecs = np.squeeze(vecs)

        return labels, vecs

def _preprocess_img(img_bytes):
    # Read from bytes to numpy array.
    img = imread(img_bytes)

    # Extremely fast resize.
    img = resize(img, 224, 224, interpolation=0)
    
    # Regular image: return.
    if img.shape[-1] == 3:
        return img
    
    # Grayscale image: repeat up to 3 channels.
    elif len(img.shape) == 2:
        return np.repeat(img[:, :, np.newaxis], 3, -1)

    # Other image: repeat first channel 3 times.
    return np.repeat(img[:, :, :1], 3, -1)

def _get_img_bytes_from_s3(args):
    bucket, key, s3_client = args
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return BytesIO(obj['Body'].read())

def _gzip_str(s):
    b = s.encode()
    g = gzip.compress(b)
    return BytesIO(g)


if __name__ == "__main__":

    ap = ArgumentParser(description="See script header")
    ap.add_argument("--kafka_sub_topic", 
                    help="Name of topic from which images are consumed", 
                    default="aknn-demo.image-objects")
    ap.add_argument("--kafka_pub_topic", 
                    help="Name of topic to which feature vectors get produced", 
                    default="aknn-demo.feature-objects")
    ap.add_argument("--s3_pub_bucket",
                    help="Name of bucket to which feature vectors get saved",
                    default="klibisz-aknn-demo")
    ap.add_argument("--kafka_servers", 
                    help="Bootstrap servers for Kafka",
                    default="ip-172-31-19-114.ec2.internal:9092")
    ap.add_argument("--kafka_group",
                    help="Group ID for Kafka consumer", 
                    default="aknn-demo.comput-convnet-features")

    args = vars(ap.parse_args())

    consumer = KafkaConsumer(
        args["kafka_sub_topic"],
        bootstrap_servers=args["kafka_servers"],
        group_id=args["kafka_group"],
        auto_offset_reset="earliest",
        key_deserializer=lambda k: k.decode(),
        value_deserializer=lambda v: json.loads(v.decode())
    )    
    producer = KafkaProducer(
        bootstrap_servers=args["kafka_servers"],
        compression_type='gzip',
        key_serializer=str.encode,
        value_serializer=str.encode)
    
    s3_client = boto3.client('s3')

    convnet = Convnet()

    pool = Pool(cpu_count())
    tpex = ThreadPoolExecutor(max_workers=12)

    for msg in consumer:
    
        print("Received batch with id %s containing %d images" % (msg.key, len(msg.value)))
        T0 = time()

        t0 = time()
        data = map(lambda o: (o['bucket'], o['key'], s3_client), msg.value)
        imgs_bytes = list(tpex.map(_get_img_bytes_from_s3, data))
        print("Download images", time() - t0)
        
        t0 = time()
        imgs_iter = pool.map(_preprocess_img, imgs_bytes)
        print("Preprocess images", time() - t0)

        t0 = time()
        labels, vecs = convnet.get_labels_and_vecs(imgs_iter)
        print("Compute features", time() - t0)

        t0 = time()
        s3_futures = []
        for img_obj, label, vec in zip(msg.value, labels, vecs):
            features_dict = dict(img_obj=img_obj, label=label, vec=list(map(float, vec)))
            features_str = json.dumps(features_dict)
            features_body = _gzip_str(features_str)
            features_key = "img-features-%s.json.gz" % (img_obj["key"].split('.')[0])
            s3_futures.append(tpex.submit(s3_client.put_object, 
                Body=features_body, Bucket=args["s3_pub_bucket"], Key=features_key))
            value = json.dumps(dict(bucket=args["s3_pub_bucket"], key=features_key))
            producer.send(args["kafka_pub_topic"], key=features_key, value=value)

        wait(s3_futures)
        print("Upload features", time() - t0)
        
        print("%s: %s %.2lf, %.2lf, %d" \
            % (msg.key, str(vecs.shape), vecs.mean(), vecs.std(), time() - T0))

    producer.flush()
