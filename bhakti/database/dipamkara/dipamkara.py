import numpy
import os
import json

from medusa.embedding import (
    COSINE,
    EUCLIDEAN_L2,
    l2_normalize,
    find_distance
)

from bhakti.const import (
    EMPTY_DICT,
    EMPTY_STR,
    EMPTY_LIST,
    UTF_8,
    NULL
)
from bhakti.database.dipamkara.dipamkara_dsl import (
    DIPAMKARA_DSL,
    DIPAMKARA_DSL_KEYWORDS,
    find_keywords_of_dipamkara_dsl
)
from bhakti.database.dipamkara.lock import (
    vector_modify_lock,
    inverted_index_modify_lock,
    document_modify_lock,
    auto_increment_lock
)
from bhakti.database.dipamkara.exception.dipamkara_vector_error import DipamkaraVectorError
from bhakti.database.dipamkara.exception.dipamkara_index_error import DipamkaraIndexError
from bhakti.database.dipamkara.exception.dipamkara_index_existence_error import DipamkaraIndexExistenceError
from bhakti.database.dipamkara.exception.dipamkara_vector_existence_error import DipamkaraVectorExistenceError
from bhakti.database.dipamkara.decorator.lock_on import lock_on


class Dipamkara:
    def __init__(
            self,
            dimension: int,
            archive_path: str,
            cached: bool = False
    ):
        # global cache option
        self.__cached = cached
        # vectors are stored as string
        # dict[key, dict[vectors_of_documents_contains_key, value]]
        self.__inverted_index: dict[str,
            dict[str, any]
        ] = EMPTY_DICT()
        # vectors are stored as string
        # dict[vector_unique, document_id]
        self.__vector: dict[str, int] = EMPTY_DICT()
        # dict[document_id, document]
        self.__document: dict[int,
        dict[str, any]
        ] = EMPTY_DICT()
        self.__archive_path = archive_path
        self.__archive_inv = os.path.join(self.__archive_path, '.inv')
        self.__archive_vec = os.path.join(self.__archive_path, '.vec')
        self.__archive_zen = os.path.join(self.__archive_path, 'zen')
        self.__auto_increment_ptr = NULL
        self.__dimension = dimension

        # todo .vec .inv 采用异步快照，.zen 采用同步存储

        if not os.path.exists(archive_path):
            os.mkdir(archive_path)
        # always cache
        if not os.path.exists(self.__archive_inv):
            with open(self.__archive_inv, 'w') as inv_file:
                inv_file.write(EMPTY_STR())
        else:
            with open(self.__archive_inv, 'r', encoding=UTF_8) as inv_file:
                _inv_file_text = inv_file.read()
            if _inv_file_text != EMPTY_STR():
                self.__inverted_index = json.loads(_inv_file_text)
        # always cache
        if not os.path.exists(self.__archive_vec):
            with open(self.__archive_vec, 'w') as vec_file:
                vec_file.write(EMPTY_STR())
        else:
            with open(self.__archive_vec, 'r', encoding=UTF_8) as vec_file:
                _vec_file_text = vec_file.read()
            if _vec_file_text != EMPTY_STR():
                self.__vector = json.loads(_vec_file_text)
        # conditional cache
        if not os.path.exists(self.__archive_zen):
            os.mkdir(self.__archive_zen)
        else:
            entries = os.listdir(self.__archive_zen)
            for entry in entries:
                self.__auto_increment_ptr = max(
                    self.__auto_increment_ptr,
                    int(entry)
                )
            # load documents into memory
            if self.__cached:
                for _id in entries:
                    _path = os.path.join(self.__archive_zen, _id)
                    with open(_path, 'r', encoding=UTF_8) as _doc:
                        _doc_text = _doc.read()
                    if _doc_text != EMPTY_STR():
                        self.__document[int(_id)] = json.loads(_doc_text)

    def get_vectors(self):
        return dict(self.__vector)

    def get_auto_increment(self):
        return self.__auto_increment_ptr

    def get_cached_docs(self):
        return dict(self.__document)

    def get_indices(self):
        return dict(self.__inverted_index)

    def get_archive_dir(self):
        return self.__archive_path

    def is_fully_cached(self):
        return self.__cached

    @lock_on(auto_increment_lock)
    async def auto_increment(self) -> None:
        self.__auto_increment_ptr += 1

    @lock_on(vector_modify_lock)
    @lock_on(inverted_index_modify_lock)
    @lock_on(document_modify_lock)
    async def create(
            self,
            vector: numpy.ndarray,
            document: dict[str, any],
            index: list[str] = None,
            cached: bool = False
    ) -> bool:
        if index is None:
            index = EMPTY_LIST()
        if vector.shape[0] != self.__dimension:
            raise DipamkaraVectorError(f'Vector {vector} is {vector.shape[0]}-dimensional '
                             f'which should be {self.__dimension}-dimensional')
        vector_str = json.dumps(vector.tolist(), ensure_ascii=True)
        # prefilter
        if vector_str in self.__vector.keys():
            raise DipamkaraVectorExistenceError(f'Vector {vector} already exists')
        for _index in index:
            if not find_keywords_of_dipamkara_dsl(_index):
                if _index not in document.keys():
                    raise DipamkaraIndexError(f'Index "{_index}" is not a key of {document.keys()}, '
                                     f'try .create_index("{_index}") '
                                     f'if you want to build index on "{_index}"')
        # create document
        doc_path = os.path.join(
            self.__archive_zen,
            str(self.__auto_increment_ptr)
        )
        doc_bytes = json.dumps(
            document,
            ensure_ascii=False
        ).encode(UTF_8)
        write_success = False
        with open(doc_path, 'wb+') as file:
            file.write(doc_bytes)
            file.flush()
            file.seek(0)
            if file.read() == doc_bytes:
                # indexing
                self.__vector[vector_str] = self.__auto_increment_ptr
                # new indices created at this call
                for _index in index:
                    # if index not exist, try index all documents other than this one
                    if _index not in self.__inverted_index.keys():
                        # await self.create_index(_index)
                        if not find_keywords_of_dipamkara_dsl(_index):
                            self.__inverted_index[_index] = EMPTY_DICT()
                            self.__update_index(index=_index, reset=False)
                    else:
                        self.__inverted_index[_index][vector_str] = document[_index]
                # previous indices created update
                for _key in document.keys():
                    if _key in self.__inverted_index.keys():
                        self.__inverted_index[_key][vector_str] = document[_key]
                # global cache or partial cache
                if self.__cached or cached:
                    self.__document[self.__auto_increment_ptr] = document
                # respond success
                write_success = True
                await self.auto_increment()
        if not write_success:
            os.remove(doc_path)
        return write_success

    @lock_on(vector_modify_lock)
    @lock_on(inverted_index_modify_lock)
    @lock_on(document_modify_lock)
    async def remove_by_vector(self, vector: str | numpy.ndarray) -> bool:
        if isinstance(vector, str):
            pass
        elif isinstance(vector, numpy.ndarray):
            vector = json.dumps(vector.tolist(), ensure_ascii=True)
        else:
            raise DipamkaraVectorError(f'Value {vector} is not a vector')
        # 这里采用宽松的检查机制，因为 indexed_remove 中没有对 vector 严格上锁，所以可能会出现 vector 不存在的情况
        # 这里我选择忽视不存在的 vector
        if vector in self.__vector.keys():
            _doc_id = self.__vector[vector]
            del self.__vector[vector]
            doc_file_path = os.path.join(self.__archive_zen, str(_doc_id))
            if os.path.exists(doc_file_path):
                os.remove(doc_file_path)
            if _doc_id in self.__document.keys():
                del self.__document[_doc_id]
            for _index in self.__inverted_index.keys():
                if vector in self.__inverted_index[_index].keys():
                    del self.__inverted_index[_index][vector]
            return True
        return False

    async def indexed_remove(self, query: str) -> bool:
        vectors = await self.indexed_query(query)
        for vector in vectors:
            await self.remove_by_vector(vector)
        return True

    # 因为 update_index 中调用了 find_doc_by_vector，所以此处为 vector 和 document 上锁
    @lock_on(vector_modify_lock)
    @lock_on(inverted_index_modify_lock)
    @lock_on(document_modify_lock)
    async def create_index(self, index: str) -> dict:
        if not find_keywords_of_dipamkara_dsl(index):
            if index in self.__inverted_index.keys():
                raise DipamkaraIndexExistenceError(f'Index "{index}" exists')
            self.__inverted_index[index] = EMPTY_DICT()
            return self.__update_index(index=index, reset=False)

    @lock_on(inverted_index_modify_lock)
    async def remove_index(self, index: str) -> bool:
        if index not in self.__inverted_index.keys():
            raise DipamkaraIndexExistenceError(f'Index "{index}" not exists')
        del self.__inverted_index[index]
        return True

    # 此处为 vector 上锁，因为 vector 在此作为唯一性索引
    # 为 document 上锁，因为 find_doc_by_vector 需要读取 document
    # 为 inverted_index 上锁，因为都上这么多锁了，不差这一个
    @lock_on(vector_modify_lock)
    @lock_on(document_modify_lock)
    @lock_on(inverted_index_modify_lock)
    async def mod_doc_by_vector(self, vector: str | numpy.ndarray, key: str, value: any):
        if isinstance(vector, numpy.ndarray):
            vector = json.dumps(vector.tolist(), ensure_ascii=True)
        elif isinstance(vector, str):
            pass
        else:
            raise DipamkaraVectorError(f'Value {vector} is not a vector')
        _doc = self.__find_doc_by_vector(vector=vector, cached=False)
        if key not in _doc.keys():
            raise KeyError(f'Key "{key}" not exists')
        await self.__save_doc_by_vector(vector=vector, doc=_doc)
        _doc[key] = value
        # update index
        if key not in self.__inverted_index.keys():
            return
        _object_dict = self.__inverted_index[key]
        for _vec in _object_dict:
            if _vec == vector:
                _object_dict[_vec] = value

    # 这里为 inverted_index 上锁，因为该方法的返回值会用做索引用于删除 document
    @lock_on(inverted_index_modify_lock)
    async def indexed_query(self, query: str) -> set[str]:
        return (DIPAMKARA_DSL
                .update_expr(expr=query)
                .update_inverted_index(inverted_index=self.__inverted_index)
                .process_serialized())

    # 此处不严格上锁
    # return vector_str: distance
    async def vector_query(
            self,
            vector: numpy.ndarray,
            metric: str,
            top_k: int
    ) -> list[tuple[str, float]]:
        _result: dict[str, float] = EMPTY_DICT()
        l2_vector_challenger = l2_normalize(vector)
        for _vec in self.__vector.keys():
            _nd_arr = numpy.asarray(json.loads(_vec))
            _result[_vec] = find_distance(
                img1=l2_normalize(_nd_arr),
                img2=l2_vector_challenger,
                metric=metric
            )
        if top_k > len(_result.keys()):
            top_k = len(_result.keys())
        return sorted(_result.items(), key=lambda item: item[1])[:top_k]

    # 方法中 indexed_query 调用拥有 inverted_index 锁
    async def indexed_vector_query(
            self,
            query: str,
            vector: numpy.ndarray,
            metric: str,
            top_k: int
    ) -> list[tuple[str, float]]:
        _result: dict[str, float] = EMPTY_DICT()
        l2_vector_challenger = l2_normalize(vector)
        vectors_challenged = await self.indexed_query(query)
        for _vec in vectors_challenged:
            _nd_arr = numpy.asarray(json.loads(_vec))
            _result[_vec] = find_distance(
                img1=l2_normalize(_nd_arr),
                img2=l2_vector_challenger,
                metric=metric
            )
        if top_k > len(_result.keys()):
            top_k = len(_result.keys())
        return sorted(_result.items(), key=lambda item: item[1])[:top_k]

    # 锁住 vector，此处 vector 作为唯一索引用于标识 document
    @lock_on(vector_modify_lock)
    @lock_on(document_modify_lock)
    async def find_document_by_vector(
            self,
            vector: numpy.ndarray,
            metric: str,
            top_k: int,
            cached: bool = False
    ) -> list[dict[str, any]]:
        knn_vectors = await self.vector_query(
            vector=vector,
            metric=metric,
            top_k=top_k
        )
        _result_set: list = EMPTY_LIST()
        for _vec_str, distance in knn_vectors:
            _result_set.append(self.__find_doc_by_vector(vector=_vec_str, cached=cached))
        return _result_set

    # 锁住 vector，此处 vector 作为唯一索引用于标识 document
    @lock_on(vector_modify_lock)
    @lock_on(document_modify_lock)
    async def find_document_by_vector_indexed(
            self,
            query: str,
            vector: numpy.ndarray,
            metric: str,
            top_k: int,
            cached: bool = False
    ) -> list[dict[str, any]]:
        knn_vectors = await self.indexed_vector_query(
            query=query,
            vector=vector,
            metric=metric,
            top_k=top_k
        )
        _result_set: list = EMPTY_LIST()
        for _vec_str, distance in knn_vectors:
            _result_set.append(self.__find_doc_by_vector(vector=_vec_str, cached=cached))
        return _result_set

    # 该方法的四处调用都为 vector 和 document 上了锁
    def __find_doc_by_vector(self, vector: str | numpy.ndarray, cached: bool) -> dict[str, any]:
        if isinstance(vector, str):
            pass
        elif isinstance(vector, numpy.ndarray):
            vector = json.dumps(vector.tolist(), ensure_ascii=True)
        else:
            raise DipamkaraVectorError(f'Value {vector} is not a vector')
        if vector not in self.__vector.keys():
            raise DipamkaraVectorExistenceError(f'Vector {vector} not exists')
        _doc_id = self.__vector[vector]
        # doc cached
        if _doc_id in self.__document.keys():
            return self.__document[_doc_id]
        # doc not cached
        else:
            with open(os.path.join(self.__archive_zen, str(_doc_id)), 'r', encoding=UTF_8) as __doc:
                __doc_text = __doc.read()
            if __doc_text != EMPTY_STR():
                __doc_dict = json.loads(__doc_text)
                if self.__cached or cached:
                    self.__document[_doc_id] = __doc_dict
                return __doc_dict

    # 该方法的两处调用都为 vector 和 inverted_index 加了锁，所以这里不加，避免死锁
    def __update_index(self, index: str, reset: bool):
        if index not in self.__inverted_index.keys():
            raise DipamkaraIndexExistenceError(f'Index "{index}" not exists')
        if reset:
            self.__inverted_index[index] = EMPTY_DICT()
        for _vec_str in self.__vector.keys():
            _doc_dict = self.__find_doc_by_vector(vector=_vec_str, cached=False)
            if index in _doc_dict.keys():
                self.__inverted_index[index][_vec_str] = _doc_dict[index]

    # 该方法只有一处调用，故不在这上锁
    async def __save_doc_by_vector(self, vector: str | numpy.ndarray, doc: dict):
        if isinstance(vector, numpy.ndarray):
            vector = json.dumps(vector.tolist(), ensure_ascii=True)
        elif isinstance(vector, str):
            pass
        else:
            raise DipamkaraVectorError(f'Value {vector} is not a vector')
        if vector not in self.__vector.keys():
            raise DipamkaraVectorExistenceError(f'Vector "{vector}" not exists')
        _doc_id = self.__vector[vector]
        _doc_path = os.path.join(self.__archive_zen, str(_doc_id))
        _doc_text = json.dumps(doc, ensure_ascii=False)
        with open(_doc_path, mode='w+', encoding=UTF_8) as _doc_file:
            # _doc_file.seek(0)
            _doc_file.write(_doc_text)
