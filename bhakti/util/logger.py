import logging

logger = logging.getLogger('bhakti')
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(levelname)s] %(asctime)s %(name)s : %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logger = logging.getLogger('bhakti.client')
logger.setLevel(logging.INFO)
