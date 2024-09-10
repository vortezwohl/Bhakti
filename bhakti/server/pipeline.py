import asyncio
import abc

from bhakti.const import EMPTY_LIST


class PipelineStage:
    def __init__(self, name: str):
        self.name: str = name

    @abc.abstractmethod
    async def do(
            self,
            data: bytes | str,
            fire: bool,
            errors: list[Exception],
            io_context: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None,
            extra_context: any
    ) -> tuple[any, any, list[Exception], bool]:
        # data, extra_context, errors, fire
        pass


class Pipeline:
    def __init__(
            self,
            queue: list[PipelineStage],
            io_context: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None,
            extra_context: any,
            data: any
    ):
        self.queue: list[PipelineStage] = queue
        self.io_context: tuple[
            asyncio.StreamReader,
            asyncio.StreamWriter
        ] = io_context
        self.data: any = data
        self.extra_context: any = extra_context
        self.fire: bool = True
        self.errors: list[Exception] = EMPTY_LIST()

    async def launch(
            self
    ) -> tuple[any, any, list[Exception]]:
        if not isinstance(self.queue, list):
            return self.data, self.extra_context, self.errors
        if len(self.queue):
            for stage in self.queue:
                self.data, self.extra_context, self.errors, self.fire = await stage.do(
                    data=self.data,
                    fire=self.fire,
                    errors=self.errors,
                    io_context=self.io_context,
                    extra_context=self.extra_context
                )
                if not self.fire:
                    break
        return self.data, self.extra_context, self.errors
