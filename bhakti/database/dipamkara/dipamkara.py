import numpy
import os
import json

from bhakti.const import (
    EMPTY_DICT,
    EMPTY_STR,
    EMPTY_LIST,
    UTF_8,
    NULL
)
from bhakti.database.dipamkara.dipamkara_dsl import (
    DIPAMKARA_DSL,
    find_keywords_of_dipamkara_dsl
)
from bhakti.database.dipamkara.lock import (
    vector_modify_lock,
    inverted_index_modify_lock,
    document_modify_lock,
    auto_increment_lock
)
from bhakti.database.dipamkara.embedding import find_distance, Metric
from bhakti.util.logger import log
from bhakti.database.dipamkara.exception.dipamkara_vector_error import DipamkaraVectorError
from bhakti.database.dipamkara.exception.dipamkara_index_error import DipamkaraIndexError
from bhakti.database.dipamkara.exception.dipamkara_index_existence_error import DipamkaraIndexExistenceError
from bhakti.database.dipamkara.exception.dipamkara_vector_existence_error import DipamkaraVectorExistenceError
from bhakti.database.dipamkara.decorator.lock_on import lock_on

__VERSION__ = "0.3.8"
__AUTHOR__ = "Vortez Wohl"


class Dipamkara:
    """
    Dipamkara is a thread-safe database engine for managing documents and vector indexing based on numpy.

    Attributes:
        dimension (int): The dimension of the vectors.
        archive_path (str): The path to store documents and indexes.
        cached (bool, optional): Whether to cache documents, defaults to False.

    Methods:
        get_vectors(): Returns a dictionary of all vectors.
        get_auto_increment(): Returns the current value of the current auto-increment.
        get_cached_docs(): Returns a dictionary of cached documents.
        get_indices(): Returns a dictionary of all inverted indices.
        get_archive_dir(): Returns the storage path.
        is_fully_cached(): Returns whether database are fully cached into memory.
        save(): Asynchronously saves vectors and indexes to files.
        auto_increment(): Increments the document ID.
        create(vector, document, index=[], cached=False): Creates a document with a vector.
        invalidate_cached_doc_by_vector(vector): Invalidates a cached document by vector.
        remove_by_vector(vector, insta_save=True): Removes a document by vector.
        indexed_remove(query): Removes documents by query.
        create_index(index): Creates an index.
        remove_index(index): Removes an index.
        mod_doc_by_vector(vector, key, value): Modifies a document by vector.
        vector_query(vector, metric, top_k): Performs a vector query.
        indexed_vector_query(query, vector, metric, top_k): Performs an indexed vector query.
        find_documents_by_vector(vector, metric, top_k, cached=False): Finds documents by vector.
        find_documents_by_vector_indexed(query, vector, metric, top_k, cached=False): Finds documents by indexed vector.
    """
    def __init__(
            self,
            dimension: int,
            archive_path: str,
            cached: bool = False
    ):
        """
        Initializes the Dipamkara instance.

        Args:
            dimension (int): The dimension of the vectors.
            archive_path (str): The path to store documents and indexes.
            cached (bool, optional): Whether to cache documents, defaults to False.
        """
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

        log.info(f'Dipamkara v{__VERSION__}')

        # .vec .inv 采用异步快照，.zen 采用同步存储
        if not os.path.exists(archive_path):
            os.mkdir(archive_path)

        # always cache
        if not os.path.exists(self.__archive_vec):
            with open(self.__archive_vec, 'w') as vec_file:
                vec_file.write(EMPTY_STR())
        else:
            log.info('Initializing vectors...')
            with open(self.__archive_vec, 'r', encoding=UTF_8) as vec_file:
                _vec_file_text = vec_file.read()
            if _vec_file_text != EMPTY_STR():
                self.__vector = json.loads(_vec_file_text)

        # always cache
        if not os.path.exists(self.__archive_inv):
            with open(self.__archive_inv, 'w') as inv_file:
                inv_file.write(EMPTY_STR())
        else:
            log.info('Initializing inverted_indices...')
            with open(self.__archive_inv, 'r', encoding=UTF_8) as inv_file:
                _inv_file_text = inv_file.read()
            if _inv_file_text != EMPTY_STR():
                self.__inverted_index = json.loads(_inv_file_text)

        # conditional cache
        if not os.path.exists(self.__archive_zen):
            os.mkdir(self.__archive_zen)
        else:
            log.info('Initializing auto_increment...')
            entries = os.listdir(self.__archive_zen)
            for entry in entries:
                self.__auto_increment_ptr = max(
                    self.__auto_increment_ptr,
                    int(entry)
                )
            # 从先前的序号加一得到当前最新文档初始序号
            self.__auto_increment_ptr += 1
            # load documents into memory
            if self.__cached:
                log.info('Caching data to memory...')
                for _id in entries:
                    _path = os.path.join(self.__archive_zen, _id)
                    with open(_path, 'r', encoding=UTF_8) as _doc:
                        _doc_text = _doc.read()
                    if _doc_text != EMPTY_STR():
                        self.__document[int(_id)] = json.loads(_doc_text)
        log.info('Dipamkara initialized')

    @property
    def vectors(self):
        """
        Retrieves a dictionary of all vectors currently stored in the system.

        Returns:
            dict: A dictionary where keys are vector representations and values are document IDs.
        """
        return dict(self.__vector)

    @property
    def latest_id(self):
        """
        Gets the current value of the auto-increment used for document IDs.

        Returns:
            int: The current value of the auto-increment.
        """
        return self.__auto_increment_ptr - 1

    @property
    def cached_docs(self):
        """
        Returns a dictionary of documents that are currently cached in memory.

        Returns:
            dict: A dictionary where keys are document IDs and values are the documents.
        """
        return dict(self.__document)

    @property
    def inverted_indices(self):
        """
        Retrieves a dictionary of all inverted indices currently stored in the system.

        Returns:
            dict: A dictionary where keys are index and values are vectors and their associated value of index.
        """
        return dict(self.__inverted_index)

    @property
    def archive_dir(self):
        """
        Gets the archive directory path where vectors, inverted indices, and documents are stored.

        Returns:
            str: The path to the archive directory.
        """
        return self.__archive_path

    @property
    def is_fully_cached(self):
        """
        Checks if the system is configured to fully cache documents in memory.

        Returns:
            bool: True if documents are fully cached, False otherwise.
        """
        return self.__cached

    async def save(self):
        """
        Asynchronously saves the current state of vectors and inverted indices to the archive files.

        This method ensures that all vector and index data is persisted to disk,
        maintaining the integrity of the data across system restarts.
        """
        with open(self.__archive_vec, 'w', encoding=UTF_8) as _vec_file:
            _vec_file.write(json.dumps(self.__vector, ensure_ascii=True))
        with open(self.__archive_inv, 'w', encoding=UTF_8) as _inv_file:
            _inv_file.write(json.dumps(self.__inverted_index, ensure_ascii=False))

    # create 后保存索引
    @lock_on(vector_modify_lock)
    @lock_on(inverted_index_modify_lock)
    @lock_on(document_modify_lock)
    async def create(
            self,
            vector: numpy.ndarray,
            document: dict[str, any],
            indices: list[str] = None,
            cached: bool = False
    ) -> bool:
        """
        Creates a new document with a specified vector and optional indices.

        Args:
            vector (numpy.ndarray): The vector to associate with the document.
            document (dict[str, any]): The document to store.
            indices (list[str], optional): A list of indices to create.
            cached (bool, optional): Whether to cache the document in memory, defaults to False.

        Returns:
            bool: True if the document was successfully created, False otherwise.

        Raises:
            DipamkaraVectorError: If the vector dimensions do not match the expected dimension.
            DipamkaraVectorExistenceError: If the vector already exists.
            DipamkaraIndexError: If an index key does not exist in the document.
        """
        if indices is None:
            indices = EMPTY_LIST()
        if vector.shape[0] != self.__dimension:
            raise DipamkaraVectorError(f'Vector {vector} is {vector.shape[0]}-dimensional '
                             f'which should be {self.__dimension}-dimensional')
        vector_str = json.dumps(vector.tolist(), ensure_ascii=True)
        # prefilter
        if vector_str in self.__vector.keys():
            raise DipamkaraVectorExistenceError(f'Vector {vector} already exists')
        for _index in indices:
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
                for _index in indices:
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
                await self.__auto_increment()
                await self.save()
        if not write_success:
            os.remove(doc_path)
        return write_success

    # 这里没有给 document 上锁，因为我只需要做删除操作，如果 document 存在则从缓存中删除，
    # 如果不存在，那么由于 auto_increment 是持续递增的， 不会存在相同的 auto_increment，这意味着如果不存在，就是已经被删除了
    @lock_on(vector_modify_lock)
    async def invalidate_cached_doc_by_vector(self, vector: numpy.ndarray | str) -> bool:
        """
        Invalidates a cached document by a vector.

        Args:
            vector (numpy.ndarray or str): The vector of the document to invalidate.

        Returns:
            bool: True if the document was successfully invalidated, False otherwise.

        Raises:
            DipamkaraVectorError: If the vector is not in the correct format.
            DipamkaraVectorExistenceError: If the vector does not exist.
        """
        if isinstance(vector, str):
            pass
        elif isinstance(vector, numpy.ndarray):
            vector = json.dumps(vector.tolist(), ensure_ascii=True)
        else:
            raise DipamkaraVectorError(f'Value {vector} is not a vector')
        if vector in self.__vector.keys():
            _doc_id = self.__vector[vector]
            if _doc_id in self.__document.keys():
                del self.__document[_doc_id]
                return True
        else:
            raise DipamkaraVectorExistenceError(f'Vector {vector} not exists')

    # remove 后条件保存索引
    @lock_on(vector_modify_lock)
    @lock_on(inverted_index_modify_lock)
    @lock_on(document_modify_lock)
    async def remove_by_vector(self, vector: numpy.ndarray | str, insta_save: bool = True) -> bool:
        """
        Removes a document by its vector and conditionally saves the index.

        Args:
            vector (numpy.ndarray or str): The vector of the document to remove.
            insta_save (bool, optional): Whether to immediately save after removal, defaults to True.

        Returns:
            bool: True if the document was successfully removed, False otherwise.

        Raises:
            DipamkaraVectorError: If the vector is not in the correct format.
        """
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
            if insta_save:
                await self.save()
            return True
        return False

    # remove 若干文档后，再保存索引
    async def indexed_remove(self, query: str) -> bool:
        """
        Removes documents based on a query and saves afterward.

        Args:
            query (str): The Dipamkara DSL expression to match documents for removal.

        Returns:
            bool: True if the documents were successfully removed, False otherwise.
        """
        vectors = await self.__indexed_query(query)
        for vector in vectors:
            await self.remove_by_vector(vector, insta_save=False)
        await self.save()
        return True

    # create index 后保存索引
    # 因为 update_index 中调用了 find_doc_by_vector，所以此处为 vector 和 document 上锁
    @lock_on(vector_modify_lock)
    @lock_on(inverted_index_modify_lock)
    @lock_on(document_modify_lock)
    async def create_index(self, index: str) -> dict:
        """
        Creates a new index for the documents.

        Args:
            index (str): The name of the index to create.

        Returns:
            dict: The created index.

        Raises:
            DipamkaraIndexExistenceError: If the index already exists.
        """
        if not find_keywords_of_dipamkara_dsl(index):
            if index in self.__inverted_index.keys():
                raise DipamkaraIndexExistenceError(f'Index "{index}" exists')
            self.__inverted_index[index] = EMPTY_DICT()
            self.__update_index(index=index, reset=False)
            await self.save()
            return self.__inverted_index[index]

    # remove index 后保存索引
    @lock_on(inverted_index_modify_lock)
    async def remove_index(self, index: str) -> bool:
        """
        Removes an existing index.

        Args:
            index (str): The name of the index to remove.

        Returns:
            bool: True if the index was successfully removed, False otherwise.

        Raises:
            DipamkaraIndexExistenceError: If the index does not exist.
        """
        if index not in self.__inverted_index.keys():
            raise DipamkaraIndexExistenceError(f'Index "{index}" not exists')
        del self.__inverted_index[index]
        await self.save()
        return True

    # mod doc 后保存索引
    # 此处为 vector 上锁，因为 vector 在此作为唯一性索引
    # 为 document 上锁，因为 find_doc_by_vector 需要读取 document
    # 为 inverted_index 上锁，因为都上这么多锁了，不差这一个
    @lock_on(vector_modify_lock)
    @lock_on(document_modify_lock)
    @lock_on(inverted_index_modify_lock)
    async def mod_doc_by_vector(self, vector: numpy.ndarray | str, key: str, value: any) -> bool:
        """
        Modifies a document's field based on its vector.

        Args:
            vector (numpy.ndarray or str): The vector or vector string identifying the document.
            key (str): The key within the document to modify.
            value (any): The new value to set for the specified key.

        Returns:
            bool: True if the document was successfully modified, False otherwise.

        Raises:
            DipamkaraVectorError: If the provided vector is not valid.
            DipamkaraVectorExistenceError: If no document is associated with the given vector.
            KeyError: If the specified key does not exist in the document.
        """
        if isinstance(vector, str):
            pass
        elif isinstance(vector, numpy.ndarray):
            vector = json.dumps(vector.tolist(), ensure_ascii=True)
        else:
            raise DipamkaraVectorError(f'Value {vector} is not a vector')
        _doc = self.__find_doc_by_vector(vector=vector, cached=False)
        if key not in _doc.keys():
            raise KeyError(f'Key "{key}" not exists')
        _doc[key] = value
        await self.__save_doc_by_vector(vector=vector, doc=_doc)
        # update index
        if key not in self.__inverted_index.keys():
            return True
        _object_dict = self.__inverted_index[key]
        for _vec_str in _object_dict.keys():
            if _vec_str == vector:
                _object_dict[_vec_str] = value
        await self.save()
        return True

    # 此处不严格上锁
    # return vector_str: distance
    async def vector_query(
            self,
            vector: numpy.ndarray,
            metric: Metric,
            top_k: int
    ) -> list[tuple[numpy.ndarray, float]]:
        """
        Performs a vector query to find the top_k nearest neighbors for a given vector.

        Args:
            vector (numpy.ndarray): The query vector.
            metric (Metric): The distance metric to use for comparison. Metrics are defined in `Metric` class.
            top_k (int): The number of nearest neighbors to return.

        Returns:
            List[Tuple[numpy.ndarray, float]]: A list of tuples, each containing a nearest neighbor vector
            and its distance to the query vector.
        """
        _result: list[tuple[numpy.ndarray, float]] = EMPTY_LIST()
        for _vec in self.__vector.keys():
            _nd_arr = numpy.asarray(json.loads(_vec))
            _tmp_tuple = _nd_arr, find_distance(
                vector1=vector,
                vector2=_nd_arr,
                metric=metric
            )
            _result.append(_tmp_tuple)
        if top_k > len(_result):
            top_k = len(_result)
        return sorted(_result, key=lambda item: item[1])[:top_k]

    # 方法中 indexed_query 调用拥有 inverted_index 锁
    async def indexed_vector_query(
            self,
            query: str,
            vector: numpy.ndarray,
            metric: Metric,
            top_k: int
    ) -> list[tuple[numpy.ndarray, float]]:
        """
        Performs a vector query on documents that match a given DSL query, finding the k nearest neighbors.

        Args:
            query (str): The query string to filter documents.
            vector (numpy.ndarray): The query vector.
            metric (Metric): The distance metric to use for comparison. Metrics are defined in `Metric` class.
            top_k (int): The number of nearest neighbors to return.

        Returns:
            List[Tuple[numpy.ndarray, float]]: A list of tuples, each containing a nearest neighbor vector that matches
            the query and its distance to the query vector.
        """
        _result: list[tuple[numpy.ndarray, float]] = EMPTY_LIST()
        vectors_challenged = await self.__indexed_query(query)
        for _vec in vectors_challenged:
            _nd_arr = numpy.asarray(json.loads(_vec))
            _tmp_tuple = _nd_arr, find_distance(
                vector1=vector,
                vector2=_nd_arr,
                metric=metric
            )
            _result.append(_tmp_tuple)
        if top_k > len(_result):
            top_k = len(_result)
        return sorted(_result, key=lambda item: item[1])[:top_k]

    # 锁住 vector，此处 vector 作为唯一索引用于标识 document
    @lock_on(vector_modify_lock)
    @lock_on(document_modify_lock)
    async def find_documents_by_vector(
            self,
            vector: numpy.ndarray,
            metric: Metric,
            top_k: int,
            cached: bool = False
    ) -> list[dict[str, any]]:
        """
        Finds documents that correspond to the top_k nearest vectors to the given vector.

        Args:
            vector (numpy.ndarray): The query vector.
            metric (Metric): The distance metric to use for comparison. Metrics are defined in `Metric` class.
            top_k (int): The number of nearest neighbors to consider.
            cached (bool, optional): Whether to cache found documents.

        Returns:
            List[Dict[str, any]]: A list of documents corresponding to the nearest neighbor vectors.
        """
        knn_vectors = await self.vector_query(
            vector=vector,
            metric=metric,
            top_k=top_k
        )
        _result_set: list = EMPTY_LIST()
        for _vec, distance in knn_vectors:
            # 返回深拷贝
            _result_set.append(
                dict(self.__find_doc_by_vector(vector=_vec, cached=cached))
            )
        return _result_set

    # 锁住 vector，此处 vector 作为唯一索引用于标识 document
    @lock_on(vector_modify_lock)
    @lock_on(document_modify_lock)
    async def find_documents_by_vector_indexed(
            self,
            query: str,
            vector: numpy.ndarray,
            metric: Metric,
            top_k: int,
            cached: bool = False
    ) -> list[dict[str, any]]:
        """
        Finds documents that correspond to the top_k nearest vectors to the given vector and match a DSL query.

        Args:
            query (str): The query string to filter documents.
            vector (numpy.ndarray): The query vector.
            metric (Metric): The distance metric to use for comparison. Metrics are defined in `Metric` class.
            top_k (int): The number of nearest neighbors to consider.
            cached (bool, optional): Whether to cache found documents.

        Returns:
            List[Dict[str, any]]: A list of documents corresponding to the nearest neighbor vectors
            that match the DSL query.
        """
        knn_vectors = await self.indexed_vector_query(
            query=query,
            vector=vector,
            metric=metric,
            top_k=top_k
        )
        _result_set: list = EMPTY_LIST()
        for _vec, distance in knn_vectors:
            # 返回深拷贝
            _result_set.append(
                dict(self.__find_doc_by_vector(vector=_vec, cached=cached))
            )
        return _result_set

    # 在线程中将无法使用异步锁上下文
    @lock_on(auto_increment_lock)
    async def __auto_increment(self) -> None:
        """
        Increments the auto-increment for document IDs.

        This method is used to ensure that each document has a unique ID when it is created.
        """
        self.__auto_increment_ptr += 1

    # 这里为 inverted_index 上锁，因为该方法的返回值会用做索引用于删除 document
    @lock_on(inverted_index_modify_lock)
    async def __indexed_query(self, query: str) -> set[str]:
        """
        Performs a Dipamkara DSL query to retrieve a set of vectors.

        Args:
            query (str): The Dipamkara DSL expression to match against the indexed documents.

        Returns:
            set[str]: A set of vector strings that match the query.
        """
        return (DIPAMKARA_DSL
                .update_expr(expr=query)
                .update_inverted_index(inverted_index=self.__inverted_index)
                .process_serialized())

    # 该方法的四处调用都为 vector 和 document 上了锁
    def __find_doc_by_vector(self, vector: numpy.ndarray | str, cached: bool) -> dict[str, any]:
        """
        Retrieves a document by its vector.

        Args:
            vector (numpy.ndarray or str): The vector or vector string identifying the document.
            cached (bool): Whether to cache found documents.

        Returns:
            dict[str, any]: The document corresponding to the vector.

        Raises:
            DipamkaraVectorError: If the provided vector is not valid.
            DipamkaraVectorExistenceError: If the given vector is not exists.
        """
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

    def __update_index(self, index: str, reset: bool) -> None:
        """
        Updates the inverted index for a specific index key.

        This method is responsible for adding all vectors to the inverted index or resetting it if necessary.
        It is called internally when creating or modifying indexes and should not be used directly.

        Args:
            index (str): The index key to update or reset.
            reset (bool): If True, the index is reset by clearing all entries. If False, existing entries are preserved.

        Raises:
            DipamkaraIndexExistenceError: If the specified index does not exist and reset is False.

        Note:
            This method assumes that locks on 'vector' and 'inverted_index' are already held to prevent deadlocks.
        """
        if index not in self.__inverted_index.keys():
            raise DipamkaraIndexExistenceError(f'Index "{index}" not exists')
        if reset:
            self.__inverted_index[index] = EMPTY_DICT()
        for _vec_str in self.__vector.keys():
            _doc_dict = self.__find_doc_by_vector(vector=_vec_str, cached=False)
            if index in _doc_dict.keys():
                self.__inverted_index[index][_vec_str] = _doc_dict[index]

    async def __save_doc_by_vector(self, vector: numpy.ndarray | str, doc: dict):
        """
        Asynchronously saves a document to the archive directory using its vector as the identifier.

        This method is intended for internal use and handles the persistence of documents.
        It ensures that documents are stored with their associated vectors in the archive directory.

        Args:
            vector (numpy.ndarray or str): The vector or vector string that uniquely identifies the document.
            doc (dict): The document data to save.

        Raises:
            DipamkaraVectorError: If the vector provided is not in a valid format.
            DipamkaraVectorExistenceError: If vector doesn't exist in the archive.

        Note:
            This method does not acquire locks as it is assumed to be called within a context
            where the necessary locks are already held to prevent data corruption.
        """
        if isinstance(vector, str):
            pass
        elif isinstance(vector, numpy.ndarray):
            vector = json.dumps(vector.tolist(), ensure_ascii=True)
        else:
            raise DipamkaraVectorError(f'Value {vector} is not a vector')
        if vector not in self.__vector.keys():
            raise DipamkaraVectorExistenceError(f'Vector "{vector}" not exists')
        _doc_id = self.__vector[vector]
        _doc_path = os.path.join(self.__archive_zen, str(_doc_id))
        _doc_text = json.dumps(doc, ensure_ascii=False)
        with open(_doc_path, mode='w', encoding=UTF_8) as _doc_file:
            # _doc_file.seek(0)
            _doc_file.write(_doc_text)
