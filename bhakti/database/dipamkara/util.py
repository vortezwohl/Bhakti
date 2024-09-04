import json

import numpy


def map_dict_keys(dict_original: dict, key_transformation_func: callable):
    dict_transformed = dict({})
    for key, value in dict_original.items():
        new_key = key_transformation_func(key)
        dict_transformed[new_key] = value
    return dict_transformed


def str_to_ndarray(arr_str: str):
    return numpy.asarray(json.loads(arr_str))
