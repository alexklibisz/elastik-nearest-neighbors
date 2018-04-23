from glob import glob
from keras.applications import Xception
from keras.applications.imagenet_utils import preprocess_input, decode_predictions
from keras.models import Model
from scipy.misc import imread, imresize, imshow
from tqdm import tqdm
import numpy as np
import pdb


def get_img(img_path, dim=224):

    assert dim % 2 == 0

    img = imread(img_path)

    # Handle grayscale.
    if len(img.shape) < 3:
        tmp = np.zeros(img.shape + (3,))
        tmp[:, :, 0] = img
        tmp[:, :, 1] = img
        tmp[:, :, 2] = img
        img = tmp

    # Resize image.
    h0, w0, d = img.shape
    h1, w1 = h0, w0
    if h0 < w0:
        h1 = dim
        w1 = int(dim * w0 / h0)
    else:
        w1 = dim
        h1 = int(dim * h0 / w0)
    assert abs((h0 / w0) - (h1 / w1)) <= 0.1, "%d %d %d %d" % (h0, w0, h1, w1)

    img = imresize(img, (h1, w1, d))

    # Crop image at the center.
    # Width > height.
    if w1 > h1:
        c = int(w1 / 2)
        o = int(dim / 2)
        img = img[:, c - o: c + o, :]
    # Height > width.
    elif h1 > w1:
        c = int(h1 / 2)
        o = int(dim / 2)
        img = img[c - o: c + o, :, :]

    assert img.shape == (dim, dim, 3), '%s, %s' % (img_path, img.shape)

    return img


imgs_dir = "/mnt/data/datasets/insight-twitter-images/images/"
vecs_path = 'twitter_vectors.npy'

img_paths = glob('%s/*.jpg' % imgs_dir)[:6000]
fp_paths = open('twitter_paths.txt', 'w')

pdb.set_trace()

dim = 224
batch_size = 500
imgs_batch = np.zeros((batch_size, dim, dim, 3))

# Instantiate model and chop off some layers.
vector_layer = "avg_pool"
m1 = Xception()
m2 = Model(inputs=m1.input, outputs=m1.get_layer(vector_layer).output)

vecs = np.zeros((len(img_paths), m2.output_shape[-1]))

for i in range(0, len(img_paths), batch_size):

    for j in range(batch_size):
        imgs_batch[j] = get_img(img_paths[i + j], dim)

    imgs_batch = preprocess_input(imgs_batch, mode='tf')
    prds_batch = m2.predict(imgs_batch)
    vecs[i:i + batch_size] = prds_batch
    fp_paths.write('\n'.join(img_paths[i:i + batch_size]) + '\n')

    print('%d-%d %.3lf %.3lf %.3lf' % (
        i, i + batch_size, prds_batch.min(),
        np.median(prds_batch), prds_batch.max()))

np.save(vecs_path, vecs)
