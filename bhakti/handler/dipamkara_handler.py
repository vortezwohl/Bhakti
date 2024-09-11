import asyncio
import json
import logging

import numpy

from bhakti.const import EMPTY_STR, UTF_8, DEFAULT_EOF
from bhakti.database.dipamkara.dipamkara import Dipamkara
from bhakti.server.pipeline import PipelineStage
from bhakti.database.db_engine import DBEngine

log = logging.getLogger("dipamkara")


# response
# state in ("Exception", "OK")
def generate_response(state: str, message: str, data: any) -> bytes:
    return json.dumps({
        'state': state,
        'message': message,
        'data': data
    }, ensure_ascii=False).encode(UTF_8) + DEFAULT_EOF


STATE_EXCEPTION = "Exception"
STATE_OK = "OK"
# meta
DB_ENGINE_FIELD = 'db_engine'
DB_OPT_FIELD = 'opt'
DB_OPT_CREATE = 'create'
DB_OPT_READ = 'read'
DB_OPT_UPDATE = 'update'
DB_OPT_DELETE = 'delete'
DB_OPT_SAVE = 'save'

DB_CMD_FIELD = 'cmd'
DB_CMD_CREATE = 'create'
DB_CMD_CREATE_INDEX = 'create_index'
DB_CMD_INVALIDATE_CACHED_DOC_BY_VECTOR = 'invalidate_cached_doc_by_vector'
DB_CMD_REMOVE_BY_VECTOR = 'remove_by_vector'
DB_CMD_INDEXED_REMOVE = 'indexed_remove'
DB_CMD_REMOVE_INDEX = 'remove_index'
DB_CMD_MOD_DOC_BY_VECTOR = 'mod_doc_by_vector'
DB_CMD_VECTOR_QUERY = 'vector_query'
DB_CMD_INDEXED_VECTOR_QUERY = 'indexed_vector_query'
DB_CMD_FIND_DOCUMENTS_BY_VECTOR = 'find_documents_by_vector'
DB_CMD_FIND_DOCUMENTS_BY_VECTOR_INDEXED = 'find_documents_by_vector_indexed'
DB_CMD_SAVE = 'save'

DB_PARAM_FIELD = 'param'
# param
DB_PARAM_VECTOR = 'vector'
DB_PARAM_DOCUMENT = 'document'
DB_PARAM_INDICES = 'indices'
DB_PARAM_CACHED = 'cached'


class DipamkaraHandler(PipelineStage):
    def __init__(self, name: str = 'dipamkara_handler'):
        super().__init__(name)

    async def do(
            self,
            data: bytes | str,
            fire: bool,
            errors: list[Exception],
            io_context: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None,
            extra_context: Dipamkara
    ) -> tuple[any, any, list[Exception], bool]:
        try:
            dipamkara_message = json.loads(data)
            if (
                    dipamkara_message.get(DB_ENGINE_FIELD, EMPTY_STR()) == DBEngine.DIPAMKARA.value and
                    dipamkara_message.get(DB_OPT_FIELD, EMPTY_STR()) == DB_OPT_CREATE and
                    dipamkara_message.get(DB_CMD_FIELD, EMPTY_STR()) == DB_CMD_CREATE
            ):
                params = dipamkara_message.get(DB_PARAM_FIELD, EMPTY_STR())
                if params != EMPTY_STR():
                    vector = numpy.asarray(json.loads(params.get(DB_PARAM_VECTOR, EMPTY_STR())))
                    document = params.get(DB_PARAM_DOCUMENT, EMPTY_STR())
                    indices = params.get(DB_PARAM_INDICES, EMPTY_STR())
                    cached = params.get(DB_PARAM_CACHED, EMPTY_STR())
                    try:
                        io_context[1].write(generate_response(
                            state=STATE_OK,
                            message=EMPTY_STR(),
                            data=await extra_context.create(
                                vector=vector,
                                document=document,
                                indices=indices,
                                cached=cached
                            )
                        ))
                    except Exception as _error:
                        io_context[1].write(generate_response(state=STATE_EXCEPTION, message=str(_error), data=False))
                        errors.append(_error)
            elif (
                    dipamkara_message.get(DB_ENGINE_FIELD, EMPTY_STR()) == DBEngine.DIPAMKARA.value and
                    dipamkara_message.get(DB_OPT_FIELD, EMPTY_STR()) == DB_OPT_SAVE and
                    dipamkara_message.get(DB_CMD_FIELD, EMPTY_STR()) == DB_CMD_SAVE
            ):
                await extra_context.save()
                io_context[1].write(generate_response(
                    state=STATE_OK,
                    message=EMPTY_STR(),
                    data=True
                ))
        except Exception as error:
            errors.append(error)
        return data, extra_context, errors, fire


'''
状态码：
Exception
OK
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
