import numpy as np

from bhakti.client import BhaktiReactiveClient
from bhakti.util.async_run import sync
from bhakti.util.func_timer import time_kept
from bhakti.util.async_repeat import async_repeat
from bhakti.database import Metric


@async_repeat(1)
async def operate():
    client = BhaktiReactiveClient(timeout=1)
    results = list()
    query_vector = np.random.randn(5)
    # results.append(query_vector)
    # results.append(await client.create(vector=query_vector, document={'age': 31, 'gender': 'unknown'}))
    # results.append(await client.create_index(index='age'))
    # results.append(await client.create_index(index='gender'))
    # results.append(await client.remove_index(index='gender'))
    results.append(await client.vector_query(vector=query_vector, metric=Metric.DEFAULT_METRIC, top_k=2))
    # results.append(await client.find_documents_by_vector_indexed(query='age > 30 && gender != "female"', vector=query_vector, metric=Metric.DEFAULT_METRIC, top_k=2))
    # results.append((await client.insight())['cached_docs'])
    # results.append((await client.insight())['inverted_indices'])
    # results.append((await client.indexed_remove('age != 0')))
    return results


@time_kept
@sync
async def test():
    return await operate()


if __name__ == '__main__':
    result_sets = test()
    none_count = 0
    success_count = 0
    for result_set in result_sets:
        for result in result_set:
            if result is not None:
                success_count += 1
                print(result)
            else:
                none_count += 1
    print(f"Success: {success_count}")
    print(f"Read timeout: {none_count}")
