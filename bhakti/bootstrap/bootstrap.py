import asyncio

from bhakti.server import NioServer
from bhakti.server.pipeline import PipelineStage
from bhakti.handler import (
    StrDecoder,
    StrDataTrim,
    InboundDataLog
)
from bhakti.const import DEFAULT_HOST, DEFAULT_PORT


def run(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT
):
    pipeline: list[PipelineStage] = list()
    pipeline.append(StrDecoder())
    pipeline.append(StrDataTrim())
    pipeline.append(InboundDataLog())
    server = NioServer(
        host=host,
        port=port,
        pipeline=pipeline
    )
    asyncio.run(server.run())


if __name__ == "__main__":
    run()
