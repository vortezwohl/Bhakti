import asyncio

from bhakti.server.pipeline import PipelineStage


class DipamkaraHandler(PipelineStage):
    def __init__(self, name: str = 'dipamkara_handler'):
        super().__init__(name)

    async def do(
            self,
            data: bytes | str,
            fire: bool,
            errors: list[Exception],
            context: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None
    ) -> tuple[any, list[Exception], bool]:

        return data, errors, fire


'''
{
    'op': 'c',
    'param': {
        
    }
}
'''
