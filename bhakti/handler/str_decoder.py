import asyncio

from bhakti.server.pipeline import PipelineStage
from bhakti.const import UTF_8


class StrDecoder(PipelineStage):
    def __init__(self, name: str = 'str_decoder'):
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
        if isinstance(data, bytes):
            return data.decode(UTF_8), extra_context, errors, fire
        else:
            return data, extra_context, errors, fire
