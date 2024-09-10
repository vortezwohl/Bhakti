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
            context: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None
    ) -> tuple[any, list[Exception], bool]:
        return data[:-1*(len(DEFAULT_EOF_STR))], errors, fire

