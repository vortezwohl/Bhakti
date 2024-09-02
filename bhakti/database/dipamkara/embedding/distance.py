import numpy

from bhakti.database.dipamkara.embedding.normalize import l2_normalize, z_score_normalize
from bhakti.database.dipamkara.embedding.metric import Metric
from bhakti.database.dipamkara.exception.dipamkara_metric_not_support_error import DipamkaraMetricNotSupportedError


def find_cosine_distance(
        source_embedding: numpy.ndarray,
        test_embedding: numpy.ndarray
) -> numpy.float64:
    a = numpy.matmul(numpy.transpose(source_embedding), test_embedding)
    b = numpy.sum(numpy.multiply(source_embedding, source_embedding))
    c = numpy.sum(numpy.multiply(test_embedding, test_embedding))
    return 1 - (a / (numpy.sqrt(b) * numpy.sqrt(c)))


def find_euclidean_distance(
        source_embedding: numpy.ndarray,
        test_embedding: numpy.ndarray
) -> numpy.float64:
    euclidean_distance = source_embedding - test_embedding
    euclidean_distance = numpy.sum(numpy.multiply(euclidean_distance, euclidean_distance))
    euclidean_distance = numpy.sqrt(euclidean_distance)
    return euclidean_distance


def find_chebyshev_distance(
        source_embedding: numpy.ndarray,
        test_embedding: numpy.ndarray
):
    return numpy.max(numpy.abs(source_embedding - test_embedding))


def find_distance(
        vector1: numpy.ndarray,
        vector2: numpy.ndarray,
        metric: Metric = Metric.DEFAULT_METRIC
) -> numpy.float64:
    if metric == Metric.COSINE:
        return find_cosine_distance(vector1, vector2)
    elif metric == Metric.EUCLIDEAN:
        return find_euclidean_distance(vector1, vector2)
    elif metric == Metric.EUCLIDEAN_L2:
        return find_euclidean_distance(
            l2_normalize(vector1), l2_normalize(vector2)
        )
    elif metric == Metric.EUCLIDEAN_Z_SCORE:
        return find_euclidean_distance(
            z_score_normalize(vector1), z_score_normalize(vector2)
        )
    elif metric == Metric.CHEBYSHEV:
        return find_chebyshev_distance(vector1, vector2)
    else:
        raise DipamkaraMetricNotSupportedError(f'Unsupported metric: {metric}')
