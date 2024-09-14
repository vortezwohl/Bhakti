import json
import logging

import numpy

from bhakti.database.dipamkara.embedding import Metric
from bhakti.exception.bhakti_remote_exception import BhaktiRemoteException
from bhakti.client.simple_reactive_client import SimpleReactiveClient
from bhakti.const import (
    DEFAULT_EOF,
    EMPTY_LIST,
    UTF_8,
    DEFAULT_TIMEOUT,
    DEFAULT_BUFFER_SIZE
)
from bhakti.database.db_engine import DBEngine

log = logging.getLogger('bhakti.client')


def parseTupleOfNdarrayFloat64(_list: list):
    return numpy.asarray(_list[0]), numpy.float64(_list[1])


class BhaktiReactiveClient(SimpleReactiveClient):
    def __init__(
            self,
            server: str = '127.0.0.1',
            port: int = 23860,
            eof: bytes = DEFAULT_EOF,
            timeout: float = DEFAULT_TIMEOUT,
            buffer_size: int = DEFAULT_BUFFER_SIZE,
            db_engine: DBEngine = DBEngine.DEFAULT_ENGINE
    ):
        super().__init__(server=server, port=port, eof=eof, timeout=timeout, buffer_size=buffer_size)
        self.__db_engine: DBEngine = db_engine
        self.__eof = eof

    def response_post_process(self, response: bytes) -> str:
        return response.decode(UTF_8)[:-1 * len(self.__eof)]

    async def make_request(self, request: dict) -> any:
        _resp_bytes = await super().send_receive(
            message=json.dumps(request, ensure_ascii=False).encode(UTF_8)
        )
        if _resp_bytes is None:
            return _resp_bytes
        _resp = json.loads(self.response_post_process(response=_resp_bytes))
        if _resp['state'] == 'Exception':
            raise BhaktiRemoteException(message=_resp['message'])
        return _resp['data']

    async def insight(self) -> dict | None:
        return await self.make_request({
            "db_engine": self.__db_engine.value,
            "opt": "insight",
            "cmd": "insight"
        })

    async def create(
            self,
            vector: numpy.ndarray,
            document: dict[str, any],
            indices: list[str] = EMPTY_LIST(),
            cached: bool = False
    ) -> bool | None:
        return await self.make_request({
            "db_engine": self.__db_engine.value,
            "opt": "create",
            "cmd": "create",
            "param": {
                "vector": vector.tolist(),
                "document": document,
                "indices": indices,
                "cached": cached
            }
        })

    async def create_index(self, index: str) -> dict | None:
        return await self.make_request({
            "db_engine": self.__db_engine.value,
            "opt": "create",
            "cmd": "create_index",
            "param": {
                "index": index
            }
        })

    async def save(self) -> bool | None:
        return await self.make_request({
            "db_engine": self.__db_engine.value,
            "opt": "save",
            "cmd": "save"
        })

    async def invalidate_cached_document_by_vector(self, vector: numpy.ndarray) -> bool | None:
        return await self.make_request({
            "db_engine": self.__db_engine.value,
            "opt": "delete",
            "cmd": "invalidate_cached_doc_by_vector",
            "param": {
                "vector": vector.tolist()
            }
        })

    async def remove_by_vector(self, vector: numpy.ndarray) -> bool | None:
        return await self.make_request({
            "db_engine": self.__db_engine.value,
            "opt": "delete",
            "cmd": "remove_by_vector",
            "param": {
                "vector": vector.tolist()
            }
        })

    async def indexed_remove(self, query: str) -> bool | None:
        return await self.make_request({
            "db_engine": self.__db_engine.value,
            "opt": "delete",
            "cmd": "indexed_remove",
            "param": {
                "query": query
            }
        })

    async def remove_index(self, index: str) -> bool | None:
        return await self.make_request({
            "db_engine": self.__db_engine.value,
            "opt": "delete",
            "cmd": "remove_index",
            "param": {
                "index": index
            }
        })

    async def modify_document_by_vector(self, vector: numpy.ndarray, key: str, value: any) -> bool | None:
        return await self.make_request({
            "db_engine": self.__db_engine.value,
            "opt": "update",
            "cmd": "mod_doc_by_vector",
            "param": {
                "vector": vector.tolist(),
                "key": key,
                "value": value
            }
        })

    async def vector_query(
            self,
            vector: numpy.ndarray,
            metric: Metric,
            top_k: int
    ) -> list[tuple[numpy.ndarray, numpy.float64]] | None:
        response = await self.make_request({
            "db_engine": self.__db_engine.value,
            "opt": "read",
            "cmd": "vector_query",
            "param": {
                "vector": vector.tolist(),
                "metric_value": metric.value,
                "top_k": top_k
            }
        })
        if response is None:
            return response
        return list(map(parseTupleOfNdarrayFloat64, response))

    async def vector_query_indexed(
            self,
            query: str,
            vector: numpy.ndarray,
            metric: Metric,
            top_k: int
    ) -> list[tuple[numpy.ndarray, numpy.float64]] | None:
        response = await self.make_request({
            "db_engine": self.__db_engine.value,
            "opt": "read",
            "cmd": "indexed_vector_query",
            "param": {
                "query": query,
                "vector": vector.tolist(),
                "metric_value": metric.value,
                "top_k": top_k
            }
        })
        if response is None:
            return response
        return list(map(parseTupleOfNdarrayFloat64, response))

    async def find_documents_by_vector(
            self,
            vector: numpy.ndarray,
            metric: Metric,
            top_k: int
    ) -> list[tuple[dict[str, any], numpy.float64]] | None:
        response = await self.make_request({
            "db_engine": self.__db_engine.value,
            "opt": "read",
            "cmd": "find_documents_by_vector",
            "param": {
                "vector": vector.tolist(),
                "metric_value": metric.value,
                "top_k": top_k
            }
        })
        if response is None:
            return response
        return list(map(lambda ls: tuple[dict, numpy.float64](ls), response))

    async def find_documents_by_vector_indexed(
            self,
            query: str,
            vector: numpy.ndarray,
            metric: Metric,
            top_k: int
    ) -> list[tuple[dict[str, any], numpy.float64]] | None:
        response = await self.make_request({
            "db_engine": self.__db_engine.value,
            "opt": "read",
            "cmd": "find_documents_by_vector_indexed",
            "param": {
                "query": query,
                "vector": vector.tolist(),
                "metric_value": metric.value,
                "top_k": top_k
            }
        })
        if response is None:
            return response
        return list(map(lambda ls: tuple[dict, numpy.float64](ls), response))
