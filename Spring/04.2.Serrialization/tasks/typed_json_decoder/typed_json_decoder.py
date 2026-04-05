import typing as tp
import json

from decimal import Decimal

STR_TO_TYPE = {'int': int, 'float': float, 'decimal': Decimal}

def _object_hook(obj: dict):
    tp = obj.get("__custom_key_type__")
    if tp is None:
        return obj
    tp = STR_TO_TYPE[tp]
    return {tp(k): v for k,v in obj.items() if k != '__custom_key_type__'}

def decode_typed_json(json_value: str) -> tp.Any:
    """
    Returns deserialized object from json string.
    Checks __custom_key_type__ in object's keys to choose appropriate type.

    :param json_value: serialized object in json format
    :return: deserialized object
    """
    return json.loads(json_value, object_hook=_object_hook)
