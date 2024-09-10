import asyncio

from bhakti.server.pipeline import PipelineStage
from bhakti.util.logger import log


class InboundDataLog(PipelineStage):
    def __init__(self, name: str = 'inbound_data_log'):
        super().__init__(name)

    async def do(
            self,
            data: bytes | str,
            fire: bool,
            errors: list[Exception],
            io_context: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None,
            extra_context: any
    ) -> tuple[any, any, list[Exception], bool]:
        output = f'Data received: {data}'
        log.debug(output)
        return data, extra_context, errors, fire
