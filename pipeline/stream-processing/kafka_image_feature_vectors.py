# Kafka consumer with following responsibilities:
# - Read S3 key from Kafka topic.
# - Compute feature vector via pre-trained convnet.
# - Publish feature vector to topic.
# See below link for description of configuration:
# https://www.confluent.io/blog/introduction-to-apache-kafka-for-python-programmers/

from confluent_kafka import Consumer, KafkaError, Producer
from io import BytesIO
from keras.models import Model
from keras.applications import ResNet50
from keras.applications.imagenet_utils import preprocess_input, decode_predictions
from scipy.misc import imread
from skimage.transform import resize
from time import time
import boto3
import numpy as np
import pdb


class ImageVectorizer(object):

    def __init__(self):
        self.resnet50 = ResNet50(include_top=False)
        self.s3_client = boto3.client('s3')

    def get_feature_vector(self, s3_key):

        # Download image from S3 into memory.
        obj = self.s3_client.get_object(Bucket='klibisz-twitter-stream', Key=s3_key)
        bod = BytesIO(obj['Body'].read())
        img = imread(bod)

        # Pre-process image for keras.
        img = resize(img, (224, 224), preserve_range=True)
        img_batch = preprocess_input(img[np.newaxis, ...], mode='caffe')

        # Compute keras output.
        vec_batch = self.resnet50.predict(img_batch)
        vec = vec_batch.reshape((vec_batch.shape[-1]))

        # Return numpy feature vector
        return vec


if __name__ == "__main__":

    K_SERVER = "localhost:9092"
    K_SUB_TOPIC = "image-s3-keys"
    K_PUB_TOPIC = "image-feature-vectors"

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
    producer = Producer({"bootstrap.servers": K_SERVER})
    consumer.subscribe([K_SUB_TOPIC])

    image_vectorizer = ImageVectorizer()

    while True:

        # TODO: is it really best practice to poll like this?
        msg = consumer.poll(0.1)
        if msg is None:
            continue

        if msg.error():
            print('Error: %s' % msg.error().str())
            continue

        # Compute feature vector.
        try:
            image_key = msg.value().decode()
            vec = image_vectorizer.get_feature_vector(image_key)
            print('%s %s %.3lf %.3lf %.3lf' % (
                image_key, str(vec.shape), vec.min(), vec.mean(), vec.max()))
            producer.produce(K_PUB_TOPIC, key=image_key, value=vec.tostring())
        except Exception as ex:
            print('Exception processing image key %s: %s' % (image_key, ex))

    # TODO: use atexit to make sure it flushes if the script fails.
    producer.flush()
