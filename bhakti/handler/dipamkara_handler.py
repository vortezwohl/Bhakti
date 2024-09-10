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
状态码：
0000: create
0001: find
0010: update
0011: remove

0000/Exception
0000/OK
...

0010:
数据库在启动服务器时选择并创建，后续客户端发送指令并执行：
{
    'opt': 'create',
    'cmd': 'create',
    'param': {
        'vector': '[1,0,1,0,1]',
        'document': {...},
        'indices': [..],
        'cached': 0
    }
} -> {
    'state': 0000/Exception,
    'message': '...',
    'data': 0
} or {
    'state': 0000/OK,
    'message': 'OK',
    'data': 1
}

{
    'opt': 'read',
    'cmd': 'vector_query',
    'param': {
        'vector': '[1,0,1,0,1]',
        'metric': Metric,
        'top_k': 4
    }
} -> {
    'state': 0000/Exception,
    'message': '...',
    'data': 0
} or {
    'state': 0000/OK,
    'message': 'OK',
    'data': [(...),(...)...]
}
...
'''
