from glob import glob
from keras.applications import MobileNet
from keras.applications.imagenet_utils import preprocess_input, decode_predictions
from keras.models import Model
from scipy.misc import imread, imresize, imshow
from tqdm import tqdm
import numpy as np
import pdb


imgs_dir = "/home/alex/Downloads/ILSVRC/Data/DET/test/"
vecs_path = 'imagenet_vectors.npy'
labels_path = 'imagenet_labels.txt'

img_paths = sorted(glob('%s/*.JPEG' % imgs_dir))
fp_labels = open(labels_path, 'w')

batch_size = 500
imgs_batch = np.zeros((batch_size, 224, 224, 3))

# Instantiate model and chop off some layers.
vector_layer = "conv_preds"
model = MobileNet()
model2 = Model(inputs=model.input, outputs=model.get_layer(vector_layer).output)
model.summary()

vecs = np.zeros((len(img_paths), model.output_shape[-1]))

for i, img_path in enumerate(img_paths):

    img = imread(img_path)

    # Handle grayscale.
    if len(img.shape) < 3:
        tmp = np.zeros(img.shape + (3,))
        tmp[:, :, 0] = img
        tmp[:, :, 1] = img
        tmp[:, :, 2] = img
        img = tmp

    # Resize and crop to fit network.
    h0, w0, d = img.shape
    h1, w1 = h0, w0
    if h0 < w0:
        h1 = 224
        w1 = int(224 * w0 / h0)
    else:
        w1 = 224
        h1 = int(224 * h0 / w0)
    assert abs((h0 / w0) - (h1 / w1)) <= 0.1, "%d %d %d %d" % (h0, w0, h1, w1)

    img = imresize(img, (h1, w1, d))

    # imgs_batch = img[np.newaxis, :224, :224, :].astype(np.float32)
    # imgs_batch = preprocess_input(imgs_batch, mode='tf')
    # prds_batch = model.predict(imgs_batch)
    # print(decode_predictions(prds_batch))
    # imshow(img)

    imgs_batch[i % batch_size] = img[:224, :224, :]

    if (i % batch_size == 0 and i > 0):
        imgs_batch = preprocess_input(imgs_batch, mode='tf')

        labels = decode_predictions(model.predict(imgs_batch))
        s = ""
        for img_path, line in zip(img_paths[i - batch_size:i], labels):
            s += img_path.split('/')[-1] + ' '
            s += ','.join(map(lambda x: x[1], line)) + '\n'
        fp_labels.write(s)

        prd = model2.predict(imgs_batch)
        vecs[i - batch_size:i] = prd.reshape(prd.shape[0], prd.shape[-1])
        print('%d %.3lf %.3lf %.3lf' % (i, prd.min(), prd.mean(), prd.max()))


np.save(vecs_path, vecs)
