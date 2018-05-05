from argparse import ArgumentParser
from glob import glob
from io import BytesIO
from keras.models import Model
from keras.applications import MobileNet
from keras.applications.imagenet_utils import preprocess_input, decode_predictions
from scipy.misc import imread, imsave
from skimage.transform import resize
from time import time
from tqdm import tqdm
import base64
import numpy as np
import pdb


class ImageVectorizer(object):

    def __init__(self):
        net = MobileNet()
        self.convnet = Model(net.input, net.get_layer('conv_preds').output)

    def get_feature_vectors(self, img_list):

        t0 = time()

        img_arr = np.zeros((len(img_list), 224, 224, 3))

        for i, img in enumerate(img_list):
            if len(img.shape) == 2:
                img = np.repeat(img[:, :, np.newaxis], 3, -1)
            img_arr[i] = resize(img[:, :, :3], (224, 224),
                                mode='reflect',
                                preserve_range=True)

        # Pre-process batch for keras.
        img_arr = preprocess_input(img_arr, mode='caffe')

        print('Preprocessing', time() - t0)

        # Compute, return feature vectors.
        t0 = time()
        vec_arr = self.convnet.predict(img_arr)
        vec_arr = np.squeeze(vec_arr)
        print('Computing', time() - t0)
        return vec_arr


if __name__ == "__main__":

    ap = ArgumentParser(description="Compute feature vectors for a large batch of images")
    ap.add_argument('images_dir', help='full path to directory where images are stored')
    ap.add_argument('index_path', help='path to index file')
    ap.add_argument('features_dir', help='full path to directory where features will be stored')
    ap.add_argument('-b', '--batch', help='batch size', default=128, type=int)
    args = vars(ap.parse_args())

    img_vectorizer = ImageVectorizer()
    img_list = []
    id_list = []

    # with open(args['index_path']) as fp:
    #     lines = fp.read().split('\n')

    for i, twitter_id, ext in map(str.split, open(args['index_path'])):

        # i, twitter_id, ext = line.split(' ')
        img_path = "%s/%s.%s" % (args['images_dir'], twitter_id, ext)

        t0 = time()

        try:
            img_list.append(imread(img_path))
            id_list.append(twitter_id)
        except Exception as ex:
            print("Error reading image %s" % img_path, ex)

        if len(img_list) < args['batch']:
            continue

        print('Reading', time() - t0)

        try:
            vec_arr = img_vectorizer.get_feature_vectors(img_list)
            img_list = []
            id_list = []
            print("---")
        except Exception as ex:
            print("Error computing vectors", ex)
