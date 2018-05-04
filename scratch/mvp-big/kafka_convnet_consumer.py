from kafka import KafkaConsumer, KafkaProducer
from io import BytesIO
from keras.models import Model
from keras.applications import ResNet50
from keras.applications.imagenet_utils import preprocess_input, decode_predictions
from scipy.misc import imread, imsave
from skimage.transform import resize
from time import time
from tqdm import tqdm
import base64
import numpy as np
import pdb

KAFKA_SERVERS = [
    "ip-172-31-19-114.ec2.internal:9092",
    "ip-172-31-18-192.ec2.internal:9092",
    "ip-172-31-20-205.ec2.internal:9092"
]
KAFKA_SUB_TOPIC = "aknn-demo-twitter-images-base64"
KAFKA_PUB_TOPIC = "aknn-demo-feature-vectors"
KAFKA_GROUP_ID = "aknn-demo-convnet-consumers"


class ImageVectorizer(object):

    def __init__(self):
        self.resnet50 = ResNet50(include_top=False)

    def get_feature_vector(self, img):

        # Pre-process image for keras.
        img = resize(img[:, :, :3], (224, 224), preserve_range=True)
        img_batch = preprocess_input(img[np.newaxis, ...], mode='caffe')

        # Compute keras output.
        vec_batch = self.resnet50.predict(img_batch)
        vec = vec_batch.reshape((vec_batch.shape[-1]))

        # Return numpy feature vector
        return vec


if __name__ == "__main__":

    consumer = KafkaConsumer(
        KAFKA_SUB_TOPIC,
        bootstrap_servers=",".join(KAFKA_SERVERS),
        group_id=KAFKA_GROUP_ID)

    producer = KafkaProducer(bootstrap_servers=",".join(KAFKA_SERVERS))

    image_vectorizer = ImageVectorizer()

    pbar = tqdm(consumer)
    for msg in pbar:
        try:
            bod = BytesIO(base64.decodebytes(msg.value))
            img = imread(bod)
            vec = image_vectorizer.get_feature_vector(img).astype(np.float16)
            producer.send(KAFKA_PUB_TOPIC, key=msg.key, value=vec.tostring())
            pbar.set_description("%s: %.2lf, %.2lf, %.2lf" % (msg.key.decode(), vec.min(), vec.mean(), vec.max()))
        except Exception as ex:
            print("Exception", msg, ex)

    producer.flush()
