import logging

from bhakti import BhaktiServer
from bhakti.database import DBEngine

if __name__ == '__main__':
    logging.getLogger("bhakti").setLevel(logging.DEBUG)
    logging.getLogger("dipamkara").setLevel(logging.DEBUG)
    BhaktiServer(
        eof=b'_wzh_',
        dimension=4096,
        db_path=r"E:\Python\bhakti\test_archive",
        db_engine=DBEngine.DIPAMKARA,
        cached=False,
        verbose=True
    ).run()
