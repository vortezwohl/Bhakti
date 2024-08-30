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
            context: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None
    ) -> tuple[any, list[Exception], bool]:
        pass


class Pipeline:
    def __init__(
            self,
            queue: list[PipelineStage],
            context: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None,
            data: any,
            fire: bool = True,
            errors: list[Exception] = EMPTY_LIST
    ):
        self.queue: list[PipelineStage] = queue
        self.context: tuple[
            asyncio.StreamReader,
            asyncio.StreamWriter
        ] = context
        self.data: any = data
        self.fire: bool = fire
        self.errors: list[Exception] = errors

    async def launch(
            self
    ) -> tuple[any, list[Exception]]:
        if len(self.queue):
            for stage in self.queue:
                self.data, self.errors, self.fire = await stage.do(
                    data=self.data,
                    fire=self.fire,
                    errors=self.errors,
                    context=self.context
                )
                if not self.fire:
                    break
        return self.data, self.errors
