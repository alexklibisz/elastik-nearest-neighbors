from keras.models import Model
from keras.applications import ResNet50
from keras.applications.imagenet_utils import preprocess_input, decode_predictions
from scipy.misc import imread, imshow
import numpy as np

img_path = "./imagenet-pizza.JPEG"
img_path = "./out.JPEG"

img = imread(img_path).astype(np.float32)
#img = img[:224, :224, :3]

img_batch = img[np.newaxis,...]
#img_batch = preprocess_input(img_batch, mode='caffe')

model = ResNet50()
prds = model.predict(img_batch)
print(decode_predictions(prds))

vector_layer = "avg_pool"
model2 = Model(inputs=model.input, outputs=model.get_layer(vector_layer).output)
prds = model2.predict(img_batch)
print(prds.shape, prds.min(), prds.mean(), prds.max())
