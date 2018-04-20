"""Based on: http://www.bogotobogo.com/Algorithms/Locality_Sensitive_Hashing_LSH_using_Cosine_Distance_Similarity.php..
Using numpy arrays instead of ints with bitwise arithmetic."""

import numpy as np
import math
import pdb


def get_signature(data, planes):
    """
    LSH signature generation using random projection
    Returns the signature bits for two data points.
    The signature bits of the two points are different
    only for the plane that divides the two points.
    """
    sig = np.zeros(len(planes), dtype=np.uint8)
    for i, p in enumerate(planes):
        sig[i] = int(np.dot(data, p) >= 0)
    return sig


def get_bitcount(xorsig):
    return xorsig.sum()


def get_xor(sig1, sig2):
    return np.bitwise_xor(sig1, sig2)


def length(v):
    """returns the length of a vector"""
    return math.sqrt(np.dot(v, v))


if __name__ == '__main__':

    dim = 50       # dimension of data points (# of features)
    bits = 1024    # number of bits (planes) per signature
    run = 999       # number of runs
    avg = 0

    # reference planes as many as bits (= signature bits)
    ref_planes = np.random.randn(bits, dim).astype(np.float16)

    for r in range(run):

        # Generate two data points p1, p2
        pt1 = np.random.randn(dim)
        pt2 = np.random.randn(dim)

        # signature bits for two data points
        sig1 = get_signature(pt1, ref_planes)
        sig2 = get_signature(pt2, ref_planes)

        # Calculates exact angle difference
        cosine = np.dot(pt1, pt2) / length(pt1) / length(pt2)
        exact = 1 - math.acos(cosine) / math.pi

        # Calculates angle difference using LSH based on cosine distance
        # It's using signature bits' count
        cosine_hash = 1 - get_bitcount(get_xor(sig1, sig2)) / bits

        # Difference between exact and LSH
        diff = abs(cosine_hash - exact) / exact
        avg += diff
        print('exact %.3f, hash %.3f, diff %.3f' % (exact, cosine_hash, diff))

    print('avg diff = %.3f' % (avg / run))
