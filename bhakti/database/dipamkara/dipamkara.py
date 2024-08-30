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
    DIPAMKARA_DSL_KEYWORDS
)


class Dipamkara:
    def __init__(
            self,
            dimension: int,
            archive_path: str,
            cached: bool = False
    ):
        # global cache option
        self.cached = cached
        # vectors are stored as string
        # dict[key, dict[vectors_of_documents_contains_key, value]]
        self.inverted_index: dict[str,
            dict[str, any]
        ] = EMPTY_DICT()
        # vectors are stored as string
        # dict[vector_unique, document_id]
        self.vector: dict[str, int] = EMPTY_DICT()
        # dict[document_id, document]
        self.document: dict[int,
        dict[str, any]
        ] = EMPTY_DICT()
        self.__archive_inv = os.path.join(archive_path, '.inv')
        self.__archive_vec = os.path.join(archive_path, '.vec')
        self.__archive_zen = os.path.join(archive_path, 'zen')
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
                self.inverted_index = json.loads(_inv_file_text)
        # always cache
        if not os.path.exists(self.__archive_vec):
            with open(self.__archive_vec, 'w') as vec_file:
                vec_file.write(EMPTY_STR())
        else:
            with open(self.__archive_vec, 'r', encoding=UTF_8) as vec_file:
                _vec_file_text = vec_file.read()
            if _vec_file_text != EMPTY_STR():
                self.vector = json.loads(_vec_file_text)
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
            if self.cached:
                for _id in entries:
                    _path = os.path.join(self.__archive_zen, _id)
                    with open(_path, 'r', encoding=UTF_8) as _doc:
                        _doc_text = _doc.read()
                    if _doc_text != EMPTY_STR():
                        self.document[int(_id)] = json.loads(_doc_text)

    def auto_increment(self) -> None:
        self.__auto_increment_ptr += 1

    async def create(
            self,
            vector: numpy.ndarray,
            document: dict[str, any],
            index: list[str] = EMPTY_LIST,
            cached: bool = False
    ) -> bool:
        if vector.shape[0] != self.__dimension:
            return False
        vector_str = json.dumps(vector.tolist(), ensure_ascii=True)
        # prefilter
        if vector_str in self.vector.keys():
            return False
        for _index in index:
            if _index not in document.keys():
                return False
            for _keyword in DIPAMKARA_DSL_KEYWORDS:
                if _index.__contains__(_keyword):
                    return False
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
                self.vector[vector_str] = self.__auto_increment_ptr
                for _index in index:
                    if _index not in self.inverted_index.keys():
                        self.inverted_index[_index] = EMPTY_DICT()
                    self.inverted_index[_index][vector_str] = document[_index]
                # global cache or partial cache
                if self.cached or cached:
                    self.document[self.__auto_increment_ptr] = document
                # respond success
                write_success = True
                self.auto_increment()
        if not write_success:
            os.remove(doc_path)
        return write_success

    async def remove_by_vector(self, vector: str | numpy.ndarray) -> bool:
        if isinstance(vector, str):
            pass
        elif isinstance(vector, numpy.ndarray):
            vector = json.dumps(vector.tolist(), ensure_ascii=True)
        else:
            raise ValueError(f'Value {vector} is not a vector')
        if vector not in self.vector.keys():
            raise IndexError(f'Vector {vector} not exists')
        _doc_id = self.vector[vector]
        del self.vector[vector]
        doc_file_path = os.path.join(self.__archive_zen, str(_doc_id))
        if os.path.exists(doc_file_path):
            os.remove(doc_file_path)
        if _doc_id in self.document.keys():
            del self.document[_doc_id]
        for _index in self.inverted_index.keys():
            if vector in self.inverted_index[_index].keys():
                del self.inverted_index[_index][vector]
        return True

    async def indexed_remove(self, query: str) -> bool:
        vectors = await self.indexed_query(query)
        for vector in vectors:
            await self.remove_by_vector(vector)
        return True

    def create_index(self, index: str) -> dict:
        if index in self.inverted_index.keys():
            raise IndexError(f'Index "{index}" exists')
        self.inverted_index[index] = EMPTY_DICT()
        return self.__update_index(index=index, reset=False)

    def remove_index(self, index: str) -> bool:
        if index not in self.inverted_index.keys():
            raise IndexError(f'Index "{index}" not exists')
        del self.inverted_index[index]
        return True

    def mod_doc_by_vector(self, vector: str | numpy.ndarray, key: str, value: any):
        if isinstance(vector, numpy.ndarray):
            vector = json.dumps(vector.tolist(), ensure_ascii=True)
        elif isinstance(vector, str):
            pass
        else:
            raise ValueError(f'Value {vector} is not a vector')
        _doc = self.__find_doc_by_vector(vector=vector, cached=False)
        if key not in _doc.keys():
            raise KeyError(f'Key "{key}" not exists')
        _doc[key] = value
        self.__save_doc_by_vector(vector=vector, doc=_doc)
        # update index
        if key not in self.inverted_index.keys():
            return
        _object_dict = self.inverted_index[key]
        for _vec in _object_dict:
            if _vec == vector:
                _object_dict[_vec] = value

    async def indexed_query(self, query: str) -> set[str]:
        return (DIPAMKARA_DSL
                .update_expr(expr=query)
                .update_inverted_index(inverted_index=self.inverted_index)
                .process_serialized())

    # return vector_str: distance
    async def vector_query(
            self,
            vector: numpy.ndarray,
            metric: str,
            top_k: int
    ) -> list[tuple[str, float]]:
        _result: dict[str, float] = EMPTY_DICT()
        l2_vector_challenger = l2_normalize(vector)
        for _vec in self.vector.keys():
            _nd_arr = numpy.asarray(json.loads(_vec))
            _result[_vec] = find_distance(
                img1=l2_normalize(_nd_arr),
                img2=l2_vector_challenger,
                metric=metric
            )
        if top_k > len(_result.keys()):
            top_k = len(_result.keys())
        return sorted(_result.items(), key=lambda item: item[1])[:top_k]

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

    def __find_doc_by_vector(self, vector: str | numpy.ndarray, cached: bool) -> dict[str, any]:
        if isinstance(vector, str):
            pass
        elif isinstance(vector, numpy.ndarray):
            vector = json.dumps(vector.tolist(), ensure_ascii=True)
        else:
            raise ValueError(f'Value {vector} is not a vector')
        if vector not in self.vector.keys():
            raise IndexError(f'Vector {vector} not exists')
        _doc_id = self.vector[vector]
        # doc cached
        if _doc_id in self.document.keys():
            return self.document[_doc_id]
        # doc not cached
        else:
            with open(os.path.join(self.__archive_zen, str(_doc_id)), 'r', encoding=UTF_8) as __doc:
                __doc_text = __doc.read()
            if __doc_text != EMPTY_STR():
                __doc_dict = json.loads(__doc_text)
                if self.cached or cached:
                    self.document[_doc_id] = __doc_dict
                return __doc_dict

    def __update_index(self, index: str, reset: bool) -> dict:
        if index not in self.inverted_index.keys():
            return EMPTY_DICT()
        if reset:
            self.inverted_index[index] = EMPTY_DICT()
        for _vec_str in self.vector.keys:
            _doc_dict = self.__find_doc_by_vector(vector=_vec_str, cached=False)
            if index in _doc_dict.keys():
                self.inverted_index[index][_vec_str] = _doc_dict[index]
        return self.inverted_index[index]

    def __save_doc_by_vector(self, vector: str | numpy.ndarray, doc: dict):
        if isinstance(vector, numpy.ndarray):
            vector = json.dumps(vector.tolist(), ensure_ascii=True)
        elif isinstance(vector, str):
            pass
        else:
            raise ValueError(f'Value {vector} is not a vector')
        if vector not in self.vector.keys():
            raise IndexError(f'Vector "{vector}" not exists')
        _doc_id = self.vector[vector]
        _doc_path = os.path.join(self.__archive_zen, str(_doc_id))
        _doc_text = json.dumps(doc, ensure_ascii=False)
        with open(_doc_path, mode='w+', encoding=UTF_8) as _doc_file:
            # _doc_file.seek(0)
            _doc_file.write(_doc_text)
