import asyncio

from bhakti.const import (
    DEFAULT_EOF,
    EMPTY_LIST,
    DEFAULT_HOST,
    DEFAULT_PORT
)
from bhakti.util.logger import log
from bhakti.server.pipeline import PipelineStage, Pipeline


class NioServer:
    def __init__(
            self,
            host: str = DEFAULT_HOST,
            port: int = DEFAULT_PORT,
            eof: bytes = DEFAULT_EOF,
            pipeline: list[PipelineStage] = EMPTY_LIST,
    ):
        self.host = host
        self.port = port
        self.eof = eof
        self.pipeline = pipeline

    async def channel_handler(
            self,
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter
    ):
        while True:
            data = await reader.readuntil(self.eof)
            if not data:
                break
            await Pipeline(
                queue=self.pipeline,
                context=(reader, writer),
                data=data
            ).launch()
        writer.close()
        await writer.wait_closed()

    async def run(self):
        server = await asyncio.start_server(
            self.channel_handler,
            self.host,
            self.port
        )
        log.info(f'Bhakti started on port {self.port}')
        async with server:
            await server.serve_forever()
