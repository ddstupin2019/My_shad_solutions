import functools
from collections.abc import Callable
from collections import OrderedDict
from typing import Any, TypeVar, cast

Function = TypeVar('Function', bound=Callable[..., Any])


def cache(max_size: int) -> Callable[[Function], Function]:
    """
    Returns decorator, which stores result of function
    for `max_size` most recent function arguments.
    :param max_size: max amount of unique arguments to store values for
    :return: decorator, which wraps any function passed
    """
    def decorator(func: Function):
        tmp_res = OrderedDict()

        @functools.wraps(func)
        def wrapper(*args, **kwds) -> Function:
            if args in tmp_res.keys():
                return tmp_res[args]
            else:
                if len(tmp_res) == max_size:
                    tmp_res.pop(list(tmp_res.keys())[0])
                tmp_res[args] = func(*args, **kwds)
                return tmp_res[args]

        return cast(Function, wrapper)

    return decorator
