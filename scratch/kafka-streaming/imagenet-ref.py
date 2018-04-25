from keras.applications import ResNet50
from keras.applications.imagenet_utils import preprocess_input, decode_predictions
from scipy.misc import imread, imshow
import numpy as np

img = imread('./imagenet-pizza.JPEG').astype(np.float32)
img = img[:224, :224, :3]

img_batch = img[np.newaxis,...]
img_batch = preprocess_input(img_batch, mode='caffe')

model = ResNet50()
prds = model.predict(img_batch)
print(decode_predictions(prds))
