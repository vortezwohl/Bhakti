import logging

from bhakti.bootstrap import start_bhakti_server
from bhakti.database.db_engine import DBEngine

if __name__ == '__main__':
    logging.getLogger("bhakti").setLevel(logging.DEBUG)
    logging.getLogger("dipamkara").setLevel(logging.DEBUG)
    start_bhakti_server(
        dimension=5,
        db_path=r"E:\Python\bhakti\test_archive",
        db_engine=DBEngine.DIPAMKARA,
        cached=False
    )
