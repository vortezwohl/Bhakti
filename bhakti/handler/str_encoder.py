import asyncio

from bhakti.server.pipeline import PipelineStage
from bhakti.const import UTF_8


class StrEncoder(PipelineStage):
    def __init__(self, name: str = 'str_encoder'):
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
        if isinstance(data, str):
            return data.encode(UTF_8), extra_context, errors, fire
        else:
            return data, extra_context, errors, fire
