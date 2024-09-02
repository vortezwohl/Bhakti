import numpy


# euclidean normalization: x / ||x||^2
def l2_normalize(x: numpy.ndarray | list) -> numpy.ndarray:
    if isinstance(x, list):
        x = numpy.array(x)
    return x / numpy.sqrt(numpy.sum(numpy.multiply(x, x)))


def z_score_normalize(x: numpy.ndarray | list) -> numpy.ndarray:
    if isinstance(x, list):
        x = numpy.array(x)
    mean = numpy.mean(x)
    std_dev = numpy.std(x)
    return (x - mean) / std_dev
