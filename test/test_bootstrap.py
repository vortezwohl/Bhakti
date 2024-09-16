from bhakti import BhaktiServer
from bhakti.database import DBEngine

if __name__ == '__main__':
    bhakti_server = BhaktiServer(
        dimension=1024,  # required, only vectors with 1024 dimensions are acceptable
        db_path=r'E:\Python\bhakti\test_archive',  # required, path where stores data, portable
        db_engine=DBEngine.DIPAMKARA,  # optional, default to dipamkara
        cached=False,  # optional, default to false
        host='0.0.0.0',  # optional, default to 0.0.0.0
        port=23860,  # optional, default to 23860
        eof=b'<eof>',  # optional, default to <eof>
        timeout=4.0,  # optional, default to 4.0 seconds
        buffer_size=256,  # optional, default to 4.0 seconds
        verbose=False  # optional, default to false
    )
    # run server
    bhakti_server.run()
