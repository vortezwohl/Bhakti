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
            context: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None
    ) -> tuple[any, list[Exception], bool]:
        output = f'Data received: {data}'
        if len(errors) > 0:
            output += f' : Errors occurred: {errors}'
        log.debug(output)
        return data, errors, fire
