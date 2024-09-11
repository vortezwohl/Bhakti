import json

from bhakti.client.simple_reactive_client import SimpleReactiveClient
from bhakti.util.async_run import sync

@sync
async def test_client():
    data_create = '''
    {
        "db_engine": "dipamkara",
        "opt": "create",
        "cmd": "create",
        "param": {
            "vector": "[1,0,1,0,1]",
            "document": {"age":21, "gender": "male"},
            "indices": ["age"],
            "cached": 0
        }
    }
    '''
    data_save = '''
    {
        "db_engine": "dipamkara",
        "opt": "save",
        "cmd": "save"
    }
    '''
    bytes = await SimpleReactiveClient().send_receive(message=data_save.encode('utf-8'))
    str = bytes.decode('utf-8')[:-1*len("__EOF__")]
    print(json.loads(str))

if __name__ == '__main__':
    test_client()