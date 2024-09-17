import asyncio
import logging

from bhakti.server.pipeline import PipelineStage

log = logging.getLogger("bhakti")


class InboundDataLog(PipelineStage):
    def __init__(self, name: str = 'inbound_data_log'):
        super().__init__(name)

    async def do(
            self,
            data: bytes | str,
            fire: bool,
            errors: list[Exception],
            io_context: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None,
            eof: bytes,
            extra_context: any
    ) -> tuple[any, any, list[Exception], bool]:
        peer = io_context[1].get_extra_info('peername')
        output = f'{len(data)} bytes received from {peer[0]}:{peer[1]}'
        log.info(output)
        return data, extra_context, errors, fire
