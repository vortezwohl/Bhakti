import asyncio
import json
import logging

import numpy

from bhakti.const import EMPTY_STR, UTF_8, DEFAULT_EOF, EMPTY_LIST
from bhakti.database.dipamkara.dipamkara import Dipamkara
from bhakti.database.dipamkara.embedding import Metric
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


def parse_metric(metric: str) -> Metric:
    for _metric in Metric:
        if _metric.value == metric:
            return _metric
    return Metric.DEFAULT_METRIC


STATE_EXCEPTION = "Exception"
STATE_OK = "OK"
# meta
DB_ENGINE_FIELD = 'db_engine'
DB_OPT_FIELD = 'opt'
# option
DB_OPT_INSIGHT = 'insight'
DB_OPT_CREATE = 'create'
DB_OPT_READ = 'read'
DB_OPT_UPDATE = 'update'
DB_OPT_DELETE = 'delete'
DB_OPT_SAVE = 'save'

DB_CMD_FIELD = 'cmd'
# method
DB_CMD_INSIGHT = 'insight'
DB_CMD_CREATE = 'create'
DB_CMD_CREATE_INDEX = 'create_index'
DB_CMD_SAVE = 'save'
DB_CMD_INVALIDATE_CACHED_DOC_BY_VECTOR = 'invalidate_cached_doc_by_vector'
DB_CMD_REMOVE_BY_VECTOR = 'remove_by_vector'
DB_CMD_INDEXED_REMOVE = 'indexed_remove'
DB_CMD_REMOVE_INDEX = 'remove_index'
DB_CMD_MOD_DOC_BY_VECTOR = 'mod_doc_by_vector'
DB_CMD_VECTOR_QUERY = 'vector_query'
DB_CMD_INDEXED_VECTOR_QUERY = 'indexed_vector_query'
DB_CMD_FIND_DOCUMENTS_BY_VECTOR = 'find_documents_by_vector'
DB_CMD_FIND_DOCUMENTS_BY_VECTOR_INDEXED = 'find_documents_by_vector_indexed'

DB_PARAM_FIELD = 'param'
# param
DB_PARAM_VECTOR = 'vector'
DB_PARAM_DOCUMENT = 'document'
DB_PARAM_INDICES = 'indices'
DB_PARAM_INDEX = 'index'
DB_PARAM_CACHED = 'cached'
DB_PARAM_QUERY = 'query'
DB_PARAM_KEY = 'key'
DB_PARAM_VALUE = 'value'
DB_PARAM_METRIC_VALUE = 'metric_value'
DB_PARAM_TOP_K = 'top_k'


# noinspection DuplicatedCode
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
            if dipamkara_message.get(DB_ENGINE_FIELD, EMPTY_STR()) == DBEngine.DIPAMKARA.value:
                if (
                        dipamkara_message.get(DB_OPT_FIELD, EMPTY_STR()) == DB_OPT_INSIGHT and
                        dipamkara_message.get(DB_CMD_FIELD, EMPTY_STR()) == DB_CMD_INSIGHT
                ):
                    try:
                        insight = {
                            "archive_dir": extra_context.archive_dir,
                            "enable_cache": extra_context.is_fully_cached,
                            "auto_increment": extra_context.latest_id,
                            "vectors": extra_context.vectors,
                            "inverted_indices": extra_context.inverted_indices,
                            "cached_docs": extra_context.cached_docs
                        }
                        io_context[1].write(generate_response(
                            state=STATE_OK,
                            message=EMPTY_STR(),
                            data=insight
                        ))
                    except Exception as _error:
                        io_context[1].write(generate_response(state=STATE_EXCEPTION, message=str(_error), data=None))
                        errors.append(_error)
                elif (
                        dipamkara_message.get(DB_OPT_FIELD, EMPTY_STR()) == DB_OPT_CREATE and
                        dipamkara_message.get(DB_CMD_FIELD, EMPTY_STR()) == DB_CMD_CREATE
                ):
                    params = dipamkara_message.get(DB_PARAM_FIELD, EMPTY_STR())
                    if params != EMPTY_STR():
                        vector = numpy.asarray(params.get(DB_PARAM_VECTOR, EMPTY_STR()))
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
                        dipamkara_message.get(DB_OPT_FIELD, EMPTY_STR()) == DB_OPT_CREATE and
                        dipamkara_message.get(DB_CMD_FIELD, EMPTY_STR()) == DB_CMD_CREATE_INDEX
                ):
                    params = dipamkara_message.get(DB_PARAM_FIELD, EMPTY_STR())
                    if params != EMPTY_STR():
                        index = params.get(DB_PARAM_INDEX, EMPTY_STR())
                        try:
                            io_context[1].write(generate_response(
                                state=STATE_OK,
                                message=EMPTY_STR(),
                                data=await extra_context.create_index(index=index)
                            ))
                        except Exception as _error:
                            io_context[1].write(generate_response(state=STATE_EXCEPTION, message=str(_error), data=None))
                            errors.append(_error)
                elif (
                        dipamkara_message.get(DB_OPT_FIELD, EMPTY_STR()) == DB_OPT_SAVE and
                        dipamkara_message.get(DB_CMD_FIELD, EMPTY_STR()) == DB_CMD_SAVE
                ):
                    try:
                        await extra_context.save()
                        io_context[1].write(generate_response(
                            state=STATE_OK,
                            message=EMPTY_STR(),
                            data=True
                        ))
                    except Exception as _error:
                        io_context[1].write(
                            generate_response(state=STATE_EXCEPTION, message=str(_error), data=False))
                        errors.append(_error)
                elif (
                        dipamkara_message.get(DB_OPT_FIELD, EMPTY_STR()) == DB_OPT_DELETE and
                        dipamkara_message.get(DB_CMD_FIELD, EMPTY_STR()) == DB_CMD_INVALIDATE_CACHED_DOC_BY_VECTOR
                ):
                    params = dipamkara_message.get(DB_PARAM_FIELD, EMPTY_STR())
                    if params != EMPTY_STR():
                        vector = numpy.asarray(params.get(DB_PARAM_VECTOR, EMPTY_STR()))
                        try:
                            io_context[1].write(generate_response(
                                state=STATE_OK,
                                message=EMPTY_STR(),
                                data=await extra_context.invalidate_cached_doc_by_vector(vector=vector)
                            ))
                        except Exception as _error:
                            io_context[1].write(
                                generate_response(state=STATE_EXCEPTION, message=str(_error), data=False))
                            errors.append(_error)
                elif (
                        dipamkara_message.get(DB_OPT_FIELD, EMPTY_STR()) == DB_OPT_DELETE and
                        dipamkara_message.get(DB_CMD_FIELD, EMPTY_STR()) == DB_CMD_REMOVE_BY_VECTOR
                ):
                    params = dipamkara_message.get(DB_PARAM_FIELD, EMPTY_STR())
                    if params != EMPTY_STR():
                        vector = numpy.asarray(params.get(DB_PARAM_VECTOR, EMPTY_STR()))
                        try:
                            io_context[1].write(generate_response(
                                state=STATE_OK,
                                message=EMPTY_STR(),
                                data=await extra_context.remove_by_vector(vector=vector, insta_save=True)
                            ))
                        except Exception as _error:
                            io_context[1].write(
                                generate_response(state=STATE_EXCEPTION, message=str(_error), data=False))
                            errors.append(_error)
                elif (
                        dipamkara_message.get(DB_OPT_FIELD, EMPTY_STR()) == DB_OPT_DELETE and
                        dipamkara_message.get(DB_CMD_FIELD, EMPTY_STR()) == DB_CMD_INDEXED_REMOVE
                ):
                    params = dipamkara_message.get(DB_PARAM_FIELD, EMPTY_STR())
                    if params != EMPTY_STR():
                        query = params.get(DB_PARAM_QUERY, EMPTY_STR())
                        try:
                            io_context[1].write(generate_response(
                                state=STATE_OK,
                                message=EMPTY_STR(),
                                data=await extra_context.indexed_remove(query=query)
                            ))
                        except Exception as _error:
                            io_context[1].write(
                                generate_response(state=STATE_EXCEPTION, message=str(_error), data=False))
                            errors.append(_error)
                elif (
                        dipamkara_message.get(DB_OPT_FIELD, EMPTY_STR()) == DB_OPT_DELETE and
                        dipamkara_message.get(DB_CMD_FIELD, EMPTY_STR()) == DB_CMD_REMOVE_INDEX
                ):
                    params = dipamkara_message.get(DB_PARAM_FIELD, EMPTY_STR())
                    if params != EMPTY_STR():
                        index = params.get(DB_PARAM_INDEX, EMPTY_STR())
                        try:
                            io_context[1].write(generate_response(
                                state=STATE_OK,
                                message=EMPTY_STR(),
                                data=await extra_context.remove_index(index=index)
                            ))
                        except Exception as _error:
                            io_context[1].write(
                                generate_response(state=STATE_EXCEPTION, message=str(_error), data=False))
                            errors.append(_error)
                elif (
                        dipamkara_message.get(DB_OPT_FIELD, EMPTY_STR()) == DB_OPT_UPDATE and
                        dipamkara_message.get(DB_CMD_FIELD, EMPTY_STR()) == DB_CMD_MOD_DOC_BY_VECTOR
                ):
                    params = dipamkara_message.get(DB_PARAM_FIELD, EMPTY_STR())
                    if params != EMPTY_STR():
                        vector = numpy.asarray(params.get(DB_PARAM_VECTOR, EMPTY_STR()))
                        key = params.get(DB_PARAM_KEY, EMPTY_STR())
                        value = params.get(DB_PARAM_VALUE, EMPTY_STR())
                        try:
                            io_context[1].write(generate_response(
                                state=STATE_OK,
                                message=EMPTY_STR(),
                                data=await extra_context.mod_doc_by_vector(vector=vector, key=key, value=value)
                            ))
                        except Exception as _error:
                            io_context[1].write(
                                generate_response(state=STATE_EXCEPTION, message=str(_error), data=False))
                            errors.append(_error)
                elif (
                        dipamkara_message.get(DB_OPT_FIELD, EMPTY_STR()) == DB_OPT_READ and
                        dipamkara_message.get(DB_CMD_FIELD, EMPTY_STR()) == DB_CMD_VECTOR_QUERY
                ):
                    params = dipamkara_message.get(DB_PARAM_FIELD, EMPTY_STR())
                    if params != EMPTY_STR():
                        vector = numpy.asarray(params.get(DB_PARAM_VECTOR, EMPTY_STR()))
                        metric = parse_metric(params.get(DB_PARAM_METRIC_VALUE, EMPTY_STR()))
                        top_k = params.get(DB_PARAM_TOP_K, EMPTY_STR())
                        _result_set_ndarray = await extra_context.vector_query(
                            vector=vector,
                            metric=metric,
                            top_k=top_k
                        )
                        _result_set_list = EMPTY_LIST()
                        for _ndarray, _distance in _result_set_ndarray:
                            _result_set_list.append((_ndarray.tolist(), _distance))
                        try:
                            io_context[1].write(generate_response(
                                state=STATE_OK,
                                message=EMPTY_STR(),
                                data=_result_set_list
                            ))
                        except Exception as _error:
                            io_context[1].write(
                                generate_response(state=STATE_EXCEPTION, message=str(_error), data=None))
                            errors.append(_error)
                # todo test indexed vector query
                elif (
                        dipamkara_message.get(DB_OPT_FIELD, EMPTY_STR()) == DB_OPT_READ and
                        dipamkara_message.get(DB_CMD_FIELD, EMPTY_STR()) == DB_CMD_INDEXED_VECTOR_QUERY
                ):
                    params = dipamkara_message.get(DB_PARAM_FIELD, EMPTY_STR())
                    if params != EMPTY_STR():
                        query = params.get(DB_PARAM_QUERY, EMPTY_STR())
                        vector = numpy.asarray(params.get(DB_PARAM_VECTOR, EMPTY_STR()))
                        metric = parse_metric(params.get(DB_PARAM_METRIC_VALUE, EMPTY_STR()))
                        top_k = params.get(DB_PARAM_TOP_K, EMPTY_STR())
                        try:
                            io_context[1].write(generate_response(
                                state=STATE_OK,
                                message=EMPTY_STR(),
                                data=await extra_context.indexed_vector_query(
                                    query=query,
                                    vector=vector,
                                    metric=metric,
                                    top_k=top_k
                                )
                            ))
                        except Exception as _error:
                            io_context[1].write(
                                generate_response(state=STATE_EXCEPTION, message=str(_error), data=None))
                            errors.append(_error)
                # todo test find documents by vector
                elif (
                        dipamkara_message.get(DB_OPT_FIELD, EMPTY_STR()) == DB_OPT_READ and
                        dipamkara_message.get(DB_CMD_FIELD, EMPTY_STR()) == DB_CMD_FIND_DOCUMENTS_BY_VECTOR
                ):
                    params = dipamkara_message.get(DB_PARAM_FIELD, EMPTY_STR())
                    if params != EMPTY_STR():
                        vector = numpy.asarray(params.get(DB_PARAM_VECTOR, EMPTY_STR()))
                        metric = parse_metric(params.get(DB_PARAM_METRIC_VALUE, EMPTY_STR()))
                        top_k = params.get(DB_PARAM_TOP_K, EMPTY_STR())
                        cached = params.get(DB_PARAM_CACHED, EMPTY_STR())
                        try:
                            io_context[1].write(generate_response(
                                state=STATE_OK,
                                message=EMPTY_STR(),
                                data=await extra_context.find_documents_by_vector(
                                    vector=vector,
                                    metric=metric,
                                    top_k=top_k,
                                    cached=cached
                                )
                            ))
                        except Exception as _error:
                            io_context[1].write(
                                generate_response(state=STATE_EXCEPTION, message=str(_error), data=None))
                            errors.append(_error)
                elif (
                        dipamkara_message.get(DB_OPT_FIELD, EMPTY_STR()) == DB_OPT_READ and
                        dipamkara_message.get(DB_CMD_FIELD, EMPTY_STR()) == DB_CMD_FIND_DOCUMENTS_BY_VECTOR_INDEXED
                ):
                    params = dipamkara_message.get(DB_PARAM_FIELD, EMPTY_STR())
                    if params != EMPTY_STR():
                        query = params.get(DB_PARAM_QUERY, EMPTY_STR())
                        vector = numpy.asarray(params.get(DB_PARAM_VECTOR, EMPTY_STR()))
                        metric = parse_metric(params.get(DB_PARAM_METRIC_VALUE, EMPTY_STR()))
                        top_k = params.get(DB_PARAM_TOP_K, EMPTY_STR())
                        cached = params.get(DB_PARAM_CACHED, EMPTY_STR())
                        try:
                            io_context[1].write(generate_response(
                                state=STATE_OK,
                                message=EMPTY_STR(),
                                data=await extra_context.find_documents_by_vector_indexed(
                                    query=query,
                                    vector=vector,
                                    metric=metric,
                                    top_k=top_k,
                                    cached=cached
                                )
                            ))
                        except Exception as _error:
                            io_context[1].write(
                                generate_response(state=STATE_EXCEPTION, message=str(_error), data=None))
                            errors.append(_error)
        except Exception as error:
            io_context[1].write(generate_response(state=STATE_EXCEPTION, message=str(error), data=None))
            errors.append(error)
        return data, extra_context, errors, fire
