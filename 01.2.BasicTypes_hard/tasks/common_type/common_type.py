def get_common_type(type1: type, type2: type) -> type:
    """
    Calculate common type according to rule, that it must have the most adequate interpretation after conversion.
    Look in tests for adequacy calibration.
    :param type1: one of [bool, int, float, complex, list, range, tuple, str] types
    :param type2: one of [bool, int, float, complex, list, range, tuple, str] types
    :return: the most concrete common type, which can be used to convert both input values
    """
    comparison = {"<class 'bool'>": 1, "<class 'int'>": 2, "<class 'float'>": 3, "<class 'complex'>": 4,
                  "<class 'list'>": 7, "<class 'range'>": 5, "<class 'tuple'>": 6, "<class 'str'>": 8, }
    a = comparison[str(type1)]
    b = comparison[str(type2)]
    if a == 5 == b:
        return type((1, 2))
    if (a <= 4 and b <= 4) or (a >= 5 and b >= 5):
        if a < b:
            return type2
        else:
            return type1
    else:
        return type('str')
