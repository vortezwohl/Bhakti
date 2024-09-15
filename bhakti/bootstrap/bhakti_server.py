import argparse
import logging
import datetime

import yaml

from bhakti.server import NioServer
from bhakti.server.pipeline import PipelineStage
from bhakti.util.async_run import sync
from bhakti.database.dipamkara.dipamkara import Dipamkara
from bhakti.database.db_engine import DBEngine
from bhakti.exception.engine_not_support_error import EngineNotSupportError
from bhakti.handler import (
    StrDecoder,
    StrDataTrim,
    InboundDataLog,
    DipamkaraHandler,
    ExceptionNotifier
)
from bhakti.const import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_EOF,
    DEFAULT_TIMEOUT,
    DEFAULT_BUFFER_SIZE,
    UTF_8
)

__VERSION__ = "0.2.8"
__AUTHOR__ = "Vortez Wohl"
log = logging.getLogger("bhakti")


class BhaktiServer:
    def __init__(
            self,
            dimension: int,
            db_path: str,
            db_engine: DBEngine = DBEngine.DEFAULT_ENGINE,
            cached: bool = False,
            host: str = DEFAULT_HOST,
            port: int = DEFAULT_PORT,
            eof: bytes = DEFAULT_EOF,
            timeout: float = DEFAULT_TIMEOUT,
            buffer_size: int = DEFAULT_BUFFER_SIZE,
            verbose: bool = False
    ):
        self._dimension = dimension
        self._db_path = db_path
        self._db_engine = db_engine
        self._cached = cached
        self._host = host
        self._port = port
        self._eof = eof
        self._timeout = timeout
        self._buffer_size = buffer_size
        if verbose:
            log.setLevel(logging.DEBUG)
            logging.getLogger('dipamkara').setLevel(logging.DEBUG)
        else:
            log.setLevel(logging.INFO)
            logging.getLogger('dipamkara').setLevel(logging.INFO)

    @sync
    async def run(self):
        start = datetime.datetime.now().timestamp()
        log.info(f'Bhakti v{__VERSION__}')
        log.debug(f'IO timeout: {self._timeout} seconds')
        log.debug(f'Buffer size: {self._buffer_size} bytes')
        log.debug(f'EOF: {self._eof}')
        log.info(f'Database engine: {self._db_engine}')
        log.info(f'Database path: {self._db_path}')
        log.info(f'Dimension: {self._dimension}')
        if self._db_engine == DBEngine.DIPAMKARA:
            _db_engine = Dipamkara(
                dimension=self._dimension,
                archive_path=self._db_path,
                cached=self._cached
            )
        else:
            raise EngineNotSupportError(f"DBEngine {self._db_engine} not supported")
        pipeline: list[PipelineStage] = list()
        pipeline.append(StrDecoder())
        pipeline.append(StrDataTrim())
        pipeline.append(InboundDataLog())
        pipeline.append(DipamkaraHandler())
        pipeline.append(ExceptionNotifier())
        server = NioServer(
            host=self._host,
            port=self._port,
            eof=self._eof,
            timeout=self._timeout,
            buffer_size=self._buffer_size,
            pipeline=pipeline,
            context=_db_engine
        )
        end = datetime.datetime.now().timestamp()
        log.info(f'Bhakti built in {((end - start) * 1000):.2f} ms:\n{server}')
        await server.run()


def start_bhakti_server_shell(**kwargs):
    for engine in DBEngine:
        if engine.value == kwargs['db_engine']:
            kwargs['db_engine'] = engine
    BhaktiServer(
        dimension=kwargs['dimension'],
        db_path=kwargs['db_path'],
        db_engine=kwargs['db_engine'],
        cached=kwargs['cached'],
        host=kwargs['host'],
        port=kwargs['port'],
        timeout=kwargs['timeout'],
        buffer_size=kwargs['buffer_size']
    ).run()


def read_config(conf: str):
    with open(conf, 'r', encoding=UTF_8) as file:
        data = yaml.safe_load(file)
    return data


def bhakti_entry_point():
    parser = argparse.ArgumentParser(description='Bhakti database server')
    parser.add_argument('config.yaml', type=str, help='Path to the configuration file (.yaml)')
    args = parser.parse_args()
    config = read_config(args.conf)
    if config['verbose'.upper()]:
        log.setLevel(logging.DEBUG)
        logging.getLogger('dipamkara').setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)
        logging.getLogger('dipamkara').setLevel(logging.INFO)
    start_bhakti_server_shell(
        dimension=config['dimension'.upper()],
        db_path=config['db_path'.upper()],
        db_engine=config['db_engine'.upper()],
        cached=config['cached'.upper()],
        host=config['host'.upper()],
        port=config['port'.upper()],
        timeout=config['timeout'.upper()],
        buffer_size=config['buffer_size'.upper()]
    )
