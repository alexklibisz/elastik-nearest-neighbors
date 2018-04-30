import numpy as np


class LSHModel(object):

    def __init__(self, seed=865, H=16):
        self.rng = np.random.RandomState(seed)
        self.H = H
        self.N = None
        self.NdotM = None

    def fit(self, X):

        # Fit by picking *bits* pairs of points and computing the planes
        # equidistant between them.
        X_sample = self.rng.choice(X.ravel(), size=(2, self.H, X.shape[-1]))

        # Midpoints for each pair of points.
        M = (X_sample[0, ...] + X_sample[1, ...]) / 2.

        # Normal vector for each pair of points.
        N = X_sample[-1, ...] - M

        # Keep them around for later.
        self.N = N
        self.NdotM = (N * M).sum(-1)

        return self

    def get_hash(self, X):
        XdotN = X.dot(self.N.T)
        H = (XdotN >= self.NdotM).astype(np.uint8)
        return H
