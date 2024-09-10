import asyncio
import json
from json import JSONDecodeError

from bhakti.const import DEFAULT_EOF, UTF_8
from bhakti.server.pipeline import PipelineStage


class DipamkaraHandler(PipelineStage):
    def __init__(self, name: str = 'dipamkara_handler'):
        super().__init__(name)

    async def do(
            self,
            data: bytes | str,
            fire: bool,
            errors: list[Exception],
            io_context: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None,
            extra_context: any
    ) -> tuple[any, any, list[Exception], bool]:
        try:
            # dipamkara_message = json.loads(data)
            # todo 实现数据库访问逻辑，并构造响应返回
            io_context[1].write(b"helloworld"+DEFAULT_EOF)
        except JSONDecodeError as json_error:
            errors.append(json_error)
        return data, extra_context, errors, fire


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
    'db_engine': 'dipamkara',
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
    'db_engine': 'dipamkara',
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
