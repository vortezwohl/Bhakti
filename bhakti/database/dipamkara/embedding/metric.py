import enum


class Metric(enum.Enum):
    COSINE = 'cosine'
    EUCLIDEAN = 'euclidean'
    EUCLIDEAN_L2 = 'euclidean_l2'
    EUCLIDEAN_Z_SCORE = 'euclidean_z_score'
    CHEBYSHEV = 'chebyshev'
    DEFAULT_METRIC = COSINE
