import asyncio
from inspect import Traceback

from bhakti.server.pipeline import PipelineStage
from bhakti.util.logger import log
from bhakti.const import EMPTY_STR


class ExceptionNotifier(PipelineStage):
    def __init__(self, name: str = 'exception_notifier'):
        super().__init__(name)

    async def do(
            self,
            data: bytes | str,
            fire: bool,
            errors: list[Exception],
            io_context: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None,
            extra_context: any
    ) -> tuple[any, any, list[Exception], bool]:
        if isinstance(errors, list):
            if len(errors) > 0:
                err_log = EMPTY_STR()
                for err in errors:
                    err_log += f'{str(type(err).__name__)}: "{str(err)}"'
                log.error(err_log)
        return data, extra_context, errors, False
