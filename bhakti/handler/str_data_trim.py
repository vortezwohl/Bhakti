import asyncio

from bhakti.server.pipeline import PipelineStage
from bhakti.const import DEFAULT_EOF_STR


class StrDataTrim(PipelineStage):
    def __init__(self, name: str = 'str_data_trim'):
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
        return data[:-1*(len(DEFAULT_EOF_STR))], extra_context, errors, fire

