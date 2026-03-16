from types import FunctionType
from typing import Any
CO_VARARGS = 4
CO_VARKEYWORDS = 8

ERR_TOO_MANY_POS_ARGS = 'Too many positional arguments'
ERR_TOO_MANY_KW_ARGS = 'Too many keyword arguments'
ERR_MULT_VALUES_FOR_ARG = 'Multiple values for arguments'
ERR_MISSING_POS_ARGS = 'Missing positional arguments'
ERR_MISSING_KWONLY_ARGS = 'Missing keyword-only arguments'
ERR_POSONLY_PASSED_AS_KW = 'Positional-only argument passed as keyword argument'


def bind_args(func: FunctionType, *args: Any, **kwargs: Any) -> dict[str, Any]:
    """Bind values from `args` and `kwargs` to corresponding arguments of `func`

    :param func: function to be inspected
    :param args: positional arguments to be bound
    :param kwargs: keyword arguments to be bound
    :return: `dict[argument_name] = argument_value` if binding was successful,
             raise TypeError with one of `ERR_*` error descriptions otherwise
    """
    result: dict[str, Any] = {}
    have_args = bool(func.__code__.co_flags & 0x04)
    have_kwargs = bool(func.__code__.co_flags & 0x08)
    args_name = ''
    kwargs_name = ''
    col_arg = func.__code__.co_argcount + \
        func.__code__.co_kwonlyargcount + have_args + have_kwargs

    if have_args:
        args_name = func.__code__.co_varnames[col_arg - 1 - have_kwargs]
        result[args_name] = []
    if have_kwargs:
        kwargs_name = func.__code__.co_varnames[col_arg - 1]
        result[kwargs_name] = {}
    if not func.__defaults__:
        func.__defaults__ = ()
    if not func.__kwdefaults__:
        func.__kwdefaults__ = {}

    for i in range(len(args)):
        if i < func.__code__.co_argcount:
            result[func.__code__.co_varnames[i]] = args[i]
        elif have_args:
            result[args_name].append(args[i])
        else:
            raise TypeError(ERR_TOO_MANY_POS_ARGS)

    for j in range(col_arg):
        i = func.__code__.co_varnames[j]
        if i in kwargs.keys():
            if j < func.__code__.co_posonlyargcount:
                if have_kwargs:
                    continue
                else:
                    raise TypeError(ERR_POSONLY_PASSED_AS_KW)
            if i in result:
                raise TypeError(ERR_MULT_VALUES_FOR_ARG)
            result[i] = kwargs[i]
            del kwargs[i]

    if len(kwargs):
        if have_kwargs:
            result[kwargs_name] = kwargs
        else:
            raise TypeError(ERR_TOO_MANY_KW_ARGS)

    for i in range(len(func.__defaults__)):
        if func.__code__.co_varnames[func.__code__.co_argcount - 1 - i] not in result:
            result[func.__code__.co_varnames[func.__code__.co_argcount - 1 - i]
                   ] = func.__defaults__[- 1 - i]

    for k, v in func.__kwdefaults__.items():
        if k not in result:
            result[k] = v

    if have_args:
        result[args_name] = tuple(result[args_name])

    for i in range(col_arg):
        if func.__code__.co_varnames[i] not in result.keys():
            if i < func.__code__.co_posonlyargcount:
                raise TypeError(ERR_MISSING_POS_ARGS)
            elif i < func.__code__.co_argcount:
                raise TypeError(ERR_MISSING_POS_ARGS)
            else:
                raise TypeError(ERR_MISSING_KWONLY_ARGS)

    return result
