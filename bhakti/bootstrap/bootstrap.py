from bhakti.server import NioServer
from bhakti.server.pipeline import PipelineStage
from bhakti.const import DEFAULT_HOST, DEFAULT_PORT
from bhakti.util.async_run import sync
from bhakti.util.logger import log
from bhakti.database.dipamkara.dipamkara import Dipamkara
from bhakti.database.db_engine import DBEngine
from bhakti.exception.engine_not_support_error import EngineNotSupportError
from bhakti.handler import (
    StrDecoder,
    StrDataTrim,
    InboundDataLog
)


@sync
async def start_db_server(
    dimension: int,
    db_path: str,
    db_engine: DBEngine = DBEngine.DEFAULT_ENGINE,
    cached: bool = False,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
):
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
    server = NioServer(
        host=host,
        port=port,
        pipeline=pipeline,
        context=_db_engine
    )
    log.info(f'Bhakti started on port {port}')
    await server.run()
