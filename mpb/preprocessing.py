# 11785 S18 HW2P1
# Jonathan Francis

import numpy as np


def sample_zero_mean(x):
    """
    Make each sample have a mean of zero by subtracting mean along the feature axis.
    :param x: float32(shape=(samples, features))
    :return: array same shape as x
    """
    length = len(x)
    return x - np.array(np.mean(x, axis=1)).T.reshape((length, 1))


def gcn(x, scale=55., bias=0.01):
    """
    GCN each sample (assume sample mean=0)
    :param x: float32(shape=(samples, features))
    :param scale: factor to scale output
    :param bias: bias for sqrt
    :return: scale * x / sqrt(bias + sample variance)
    """
    length = len(x)
    sample_variance = np.mean(abs(x)**2, axis=1).reshape((length, 1))
    return (scale * x) / np.sqrt(bias + sample_variance)


def feature_zero_mean(x, xtest):
    """
    Make each feature have a mean of zero by subtracting mean along sample axis.
    Use train statistics to normalize test data.
    :param x: float32(shape=(samples, features))
    :param xtest: float32(shape=(samples, features))
    :return: tuple (x, xtest)
    """
    train_mean = np.mean(x, axis=0)
    return (x-train_mean, (xtest-train_mean if xtest is not None else None))


def zca(x, xtest, bias=0.1):
    """
    ZCA training data. Use train statistics to normalize test data.
    :param x: float32(shape=(samples, features)) (assume mean=0)
    :param xtest: float32(shape=(samples, features))
    :param bias: bias to add to covariance matrix
    :return: tuple (x, xtest)
    """
    shape = np.array(x).shape
    sigma = x.T.dot(x)/shape[0] + np.eye(shape[1], shape[1])*bias

    U, S, V = np.linalg.svd(sigma)
    epsilon = 0  # 1e-5
    ZCAMatrix = np.dot(U, np.dot(np.diag(1.0/np.sqrt(S + epsilon)), U.T))
    x_zca = x.dot(ZCAMatrix)
    xtest_zca = (xtest.dot(ZCAMatrix) if xtest is not None else None)
    return (x_zca, xtest_zca)


def cifar_10_preprocess(x, xtest, image_size=32):
    print("Pre-processing data")
    """
    1) sample_zero_mean and gcn xtrain and xtest.
    2) feature_zero_mean xtrain and xtest.
    3) zca xtrain and xtest.
    4) reshape xtrain and xtest into NCHW
    :param x: float32 flat images (n, 3*image_size^2)
    :param xtest float32 flat images (n, 3*image_size^2)
    :param image_size: height and width of image
    :return: tuple (new x, new xtest), each shaped (n, 3, image_size, image_size)
    """
    # 1a sample zero mean
    x = sample_zero_mean(x)

    # 1b gcn
    x = gcn(x, scale=55., bias=0.01)
    if xtest is not None:
        xtest = sample_zero_mean(xtest)
        xtest = gcn(xtest, scale=55., bias=0.01)
    else:
        xtest = None

    # 2 feature zero mean
    x, xtest = feature_zero_mean(x, xtest)

    # 3 zca
    x, xtest = zca(x, xtest, bias=0.1)

    # 4 reshape
    shape = x.shape
    num_samples = shape[0]
    image_size = int(np.sqrt(shape[1] / 3))
    x = x.reshape(num_samples, 3, image_size, image_size)
    xtest = (xtest.reshape(-1, 3, image_size, image_size) if xtest is not None else None)

    return (x, xtest)