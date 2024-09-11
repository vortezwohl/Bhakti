import asyncio
import logging

from bhakti.const import (
    DEFAULT_EOF,
    EMPTY_LIST,
    DEFAULT_HOST,
    DEFAULT_PORT
)
from bhakti.const.bhakti_logo import BHAKTI_LOGO
from bhakti.server.pipeline import PipelineStage, Pipeline

__VERSION__ = "0.2.3"
__AUTHOR__ = "Vortez Wohl"
log = logging.getLogger("bhakti")


class NioServer:
    def __init__(
            self,
            host: str = DEFAULT_HOST,
            port: int = DEFAULT_PORT,
            eof: bytes = DEFAULT_EOF,
            pipeline: list[PipelineStage] = EMPTY_LIST,
            context: any = None
    ):
        self.context = context
        self.host = host
        self.port = port
        self.eof = eof
        self.pipeline = pipeline
        log.info(f'Bhakti v{__VERSION__}')

    def __str__(self):
        _host_str = f'Host:{self.host}'
        _port_str = f'Port:{self.port}'
        _str = (f'{BHAKTI_LOGO}'
                f'|{_host_str:^36}|\n'
                f'|{_port_str:^36}|')
        return _str

    async def channel_handler(
            self,
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter
    ):
        data = await reader.readuntil(self.eof)
        res = await Pipeline(
            queue=self.pipeline,
            io_context=(reader, writer),
            extra_context=self.context,
            data=data
        ).launch()
        # extra context
        self.context = res[1]
        writer.close()
        await writer.wait_closed()

    async def run(self):
        server = await asyncio.start_server(
            self.channel_handler,
            self.host,
            self.port
        )
        async with server:
            await server.serve_forever()
