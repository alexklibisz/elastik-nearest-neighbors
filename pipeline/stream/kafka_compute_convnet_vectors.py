from argparse import ArgumentParser
from kafka import KafkaConsumer, KafkaProducer
from io import BytesIO
from scipy.misc import imread, imsave
from skimage.transform import resize
from tqdm import tqdm
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

    def _prep_img(self, img):
        if len(img.shape) == 2:
            img = np.repeat(img[:, :, np.newaxis], 3, -1)
        if img.shape[-1] > 3:
            img = img[:,:,:3]
        return resize(img, (224, 224), mode='reflect', preserve_range=True)
    
    def get_labels_and_vecs(self, imgs_iter):
        imgs = np.array([self._prep_img(x) for x in imgs_iter])
        imgs = preprocess_input(imgs, mode=self.preprocess_mode)
        clsf, vecs = self.model.predict(imgs)
        labels = [' '.join([y[1].lower() for y in x]) for x in decode_predictions(clsf, top=10)]
        return labels, np.squeeze(vecs)


if __name__ == "__main__":

    ap = ArgumentParser(description="See script header")
    ap.add_argument("--kafka_sub_topic", 
                    help="Name of topic from which images are consumed", 
                    default="aknn-demo.images")
    ap.add_argument("--kafka_pub_topic", 
                    help="Name of topic to which feature vectors get produced", 
                    default="aknn-demo.convnet-features")
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
        auto_offset_reset="earliest")
    

#    producer = KafkaProducer(bootstrap_servers=",".join(KAFKA_SERVERS))
    convnet = Convnet()

    print('Consuming...')
    progbar = tqdm(consumer)
    for msg in progbar:
        imgs_iter = [imread(BytesIO(msg.value))]
        labels, vecs = convnet.get_labels_and_vecs(imgs_iter)
        print("%s: %s %.2lf, %.2lf, %.2lf %.2lf" \
            % (msg.key.decode(), str(vecs.shape), vecs.min(), vecs.mean(), vecs.max(), vecs.std()))
