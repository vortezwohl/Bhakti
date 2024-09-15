import logging

from bhakti.client.bhakti_reactive_client import BhaktiReactiveClient
from bhakti.database.db_engine import DBEngine
from bhakti.const import DEFAULT_TIMEOUT, DEFAULT_BUFFER_SIZE, DEFAULT_PORT, DEFAULT_EOF

log = logging.getLogger("bhakti.client")


class BhaktiClient(BhaktiReactiveClient):
    def __init__(
            self,
            server: str = '127.0.0.1',
            port: int = DEFAULT_PORT,
            eof: bytes = DEFAULT_EOF,
            timeout: float = DEFAULT_TIMEOUT,
            buffer_size: int = DEFAULT_BUFFER_SIZE,
            db_engine: DBEngine = DBEngine.DEFAULT_ENGINE,
            verbose: bool = False
    ):
        super().__init__(
            server=server,
            port=port,
            eof=eof,
            timeout=timeout,
            buffer_size=buffer_size,
            db_engine=db_engine
        )
        if verbose:
            log.setLevel(logging.DEBUG)
        else:
            log.setLevel(logging.INFO)
