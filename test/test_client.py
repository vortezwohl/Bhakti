import json

from bhakti.client.simple_reactive_client import SimpleReactiveClient
from bhakti.util.async_run import sync
from bhakti.util.func_timer import time_kept

data_create = {
    "db_engine": "dipamkara",
    "opt": "create",
    "cmd": "create",
    "param": {
        "vector": [1,0,1,0,7],
        "document": {"age":23, "gender": "male"},
        "indices": ["gender"],
        "cached": 0
    }
}
data_create_index = {
    "db_engine": "dipamkara",
    "opt": "create",
    "cmd": "create_index",
    "param": {
        "index": "age"
    }
}
data_save = {
    "db_engine": "dipamkara",
    "opt": "save",
    "cmd": "save"
}
data_insight = {
    "db_engine": "dipamkara",
    "opt": "insight",
    "cmd": "insight"
}
data_invalidate_cached_doc_by_vector = {
    "db_engine": "dipamkara",
    "opt": "delete",
    "cmd": "invalidate_cached_doc_by_vector",
    "param": {
        "vector": [1, 0, 1, 1, 1]
    }
}
data_remove_by_vector = {
    "db_engine": "dipamkara",
    "opt": "delete",
    "cmd": "remove_by_vector",
    "param": {
        "vector": [1, 0, 1, 0, 3]
    }
}
data_indexed_remove = {
    "db_engine": "dipamkara",
    "opt": "delete",
    "cmd": "indexed_remove",
    "param": {
        "query": 'gender == "male"'
    }
}
data_remove_index = {
    "db_engine": "dipamkara",
    "opt": "delete",
    "cmd": "remove_index",
    "param": {
        "index": 'age'
    }
}
data_mod_doc_by_vector = {
    "db_engine": "dipamkara",
    "opt": "update",
    "cmd": "mod_doc_by_vector",
    "param": {
        "vector": [1,0,1,0,4],
        "key": 'age',
        "value": 22
    }
}
data_vector_query = {
    "db_engine": "dipamkara",
    "opt": "read",
    "cmd": "vector_query",
    "param": {
        "vector": [1,0,1,0,1],
        "metric_value": "cosine",
        "top_k": 3
    }
}


@time_kept
@sync
async def test_client(data: dict):
    res_bytes = await SimpleReactiveClient().send_receive(message=json.dumps(data).encode('utf-8'))
    return json.loads(res_bytes.decode('utf-8')[:-1*len("__EOF__")])


if __name__ == '__main__':
    print(test_client(data_vector_query))
    print(test_client(data_insight))
