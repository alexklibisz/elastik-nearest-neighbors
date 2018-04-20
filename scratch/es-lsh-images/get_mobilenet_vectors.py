from glob import glob
from keras.applications import MobileNet
from keras.applications.imagenet_utils import preprocess_input
from keras.models import Model
from scipy.misc import imread, imresize
from tqdm import tqdm
import numpy as np
import pdb


imgs_dir = "/home/alex/Downloads/ILSVRC/Data/DET/test/"
img_paths = sorted(glob('%s/*.JPEG' % imgs_dir))

batch_size = 50

# Instantiate model and chop off some layers.
# vector_layer = "global_average_pooling2d_1"
vector_layer = "conv_preds"
model = MobileNet()
model = Model(inputs=model.input, outputs=model.get_layer(vector_layer).output)
model.summary()

vecs = np.zeros((len(img_paths), model.output_shape[-1]))

imgs_batch = np.zeros((batch_size, 224, 224, 3))

for i, img_path in tqdm(enumerate(img_paths)):

    img = imread(img_path)

    # Skip grayscale.
    if len(img.shape) < 3:
        continue

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
    imgs_batch[i % batch_size] = img[:224, :224, :]

    if (i % batch_size == 0 and i > 0):
        imgs_batch = preprocess_input(imgs_batch, mode='tf')
        vecs_batch = model.predict(imgs_batch)
        vecs_batch = vecs_batch.reshape(vecs_batch.shape[0], vecs_batch.shape[-1])
        print('%d %.3lf %.3lf %.3lf' % (i, vecs_batch.min(), vecs_batch.mean(), vecs_batch.max()))
