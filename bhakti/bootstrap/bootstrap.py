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

log = logging.getLogger("bhakti")


@sync
async def start_bhakti_server(
    dimension: int,
    db_path: str,
    db_engine: DBEngine = DBEngine.DEFAULT_ENGINE,
    cached: bool = False,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    eof: bytes = DEFAULT_EOF,
    timeout: float = DEFAULT_TIMEOUT,
    buffer_size: int = DEFAULT_BUFFER_SIZE
):
    start = datetime.datetime.now().timestamp()
    log.info(f'Database server: Bhakti')
    log.debug(f'IO timeout: {timeout} seconds')
    log.debug(f'Buffer size: {buffer_size} bytes')
    log.info(f'Database engine: {db_engine}')
    log.info(f'Data path: {db_path}')
    log.info(f'Dimension: {dimension}')
    if db_engine == DBEngine.DIPAMKARA:
        _db_engine = Dipamkara(
            dimension=dimension,
            archive_path=db_path,
            cached=cached
        )
    else:
        raise EngineNotSupportError(f"DBEngine {db_engine} not supported")
    pipeline: list[PipelineStage] = list()
    pipeline.append(StrDecoder())
    pipeline.append(StrDataTrim())
    pipeline.append(InboundDataLog())
    pipeline.append(DipamkaraHandler())
    pipeline.append(ExceptionNotifier())
    server = NioServer(
        host=host,
        port=port,
        eof=eof,
        timeout=timeout,
        buffer_size=buffer_size,
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
    start_bhakti_server(
        dimension=kwargs['dimension'],
        db_path=kwargs['db_path'],
        db_engine=kwargs['db_engine'],
        cached=kwargs['cached'],
        host=kwargs['host'],
        port=kwargs['port'],
        timeout=kwargs['timeout'],
        buffer_size=kwargs['buffer_size']
    )


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
