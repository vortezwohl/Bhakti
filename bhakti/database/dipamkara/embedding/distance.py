import numpy

from bhakti.database.dipamkara.embedding.normalize import l2_normalize
from bhakti.database.dipamkara.exception.dipamkara_metric_not_support_error import DipamkaraMetricNotSupportedError

COSINE = 'cosine'
EUCLIDEAN = 'euclidean'
EUCLIDEAN_L2 = 'euclidean_l2'
DEFAULT_METRIC = COSINE


def find_cosine_distance(
    source_embedding: numpy.ndarray, test_embedding: numpy.ndarray
) -> numpy.float64:
    a = numpy.matmul(numpy.transpose(source_embedding), test_embedding)
    b = numpy.sum(numpy.multiply(source_embedding, source_embedding))
    c = numpy.sum(numpy.multiply(test_embedding, test_embedding))
    return 1 - (a / (numpy.sqrt(b) * numpy.sqrt(c)))


def find_euclidean_distance(
    source_embedding: numpy.ndarray, test_embedding: numpy.ndarray
) -> numpy.float64:
    euclidean_distance = source_embedding - test_embedding
    euclidean_distance = numpy.sum(numpy.multiply(euclidean_distance, euclidean_distance))
    euclidean_distance = numpy.sqrt(euclidean_distance)
    return euclidean_distance


def find_distance(
        vector1: numpy.ndarray,
        vector2: numpy.ndarray,
        metric: str = DEFAULT_METRIC
) -> numpy.float64:
    if metric == COSINE:
        return find_cosine_distance(vector1, vector2)
    elif metric == EUCLIDEAN:
        return find_euclidean_distance(vector1, vector2)
    elif metric == EUCLIDEAN_L2:
        return find_euclidean_distance(
            l2_normalize(vector1), l2_normalize(vector2)
        )
    else:
        raise DipamkaraMetricNotSupportedError(f'Unsupported metric: {metric}')
