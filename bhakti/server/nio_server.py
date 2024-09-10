import asyncio

from bhakti.const import (
    DEFAULT_EOF,
    EMPTY_LIST,
    DEFAULT_HOST,
    DEFAULT_PORT,
    EMPTY_STR
)
from bhakti.const.bhakti_logo import BHAKTI_LOGO
from bhakti.server.pipeline import PipelineStage, Pipeline


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

    def __str__(self):
        _host_str = f'Host:{self.host}'
        _port_str = f'Port:{self.port}'
        _str = (f'{BHAKTI_LOGO}'
                f'|{_host_str:^37}|\n'
                f'|{_port_str:^37}|')
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
