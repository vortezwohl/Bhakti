import logging

from bhakti.bootstrap.bootstrap import start_db_server
from bhakti.database.db_engine import DBEngine

if __name__ == '__main__':
    logging.getLogger("bhakti").setLevel(logging.DEBUG)
    logging.getLogger("dipamkara").setLevel(logging.DEBUG)
    start_db_server(
        dimension=5,
        db_path=r"D:\Users\Administrator\Desktop\Project\Python\bhakti\test_archive",
        db_engine=DBEngine.DIPAMKARA,
        cached=False
    )
