import typing as tp


def convert_to_common_type(data: list[tp.Any]) -> list[tp.Any]:
    """
    Takes list of multiple types' elements and convert each element to common type according to given rules
    :param data: list of multiple types' elements
    :return: list with elements converted to common type
    """
    comparison = {"<class 'bool'>": 1, "<class 'int'>": 2, "<class 'float'>": 3, "<class 'tuple'>": 4,
                  "<class 'list'>": 6, "<class 'str'>": 5,
                  "<class 'NoneType'>": 0, }
    result = []
    q = [comparison[str(type(i))] for i in data if i == 0 or i]
    if not q:
        q.append(5)
    g_type = max(q)
    if g_type <= 3:
        w = True
        for i in data:
            if type(i) is int or type(i) is float:
                if i == 1 or i == 0 or i == 1.0 or i == 0.0:
                    w = True
                else:
                    w = False
                    break
        if w:
            g_type = 1

    if g_type == 6 or g_type == 4:
        for i in data:
            if not i:
                result.append([])
            elif type(i) is str or type(i) is int or type(i) is bool:
                result.append([i])
            else:
                result.append(list(i))
    elif g_type == 5:
        for i in data:
            if not i:
                result.append("")
            else:
                result.append(str(i))
    elif g_type == 2:
        for i in data:
            if not i:
                result.append(0)
            else:
                result.append(int(i))
    elif g_type == 3:
        for i in data:
            if not i:
                result.append(0.0)
            else:
                result.append(float(i))
    elif g_type == 1:
        for i in data:
            if not i:
                result.append(False)
            else:
                result.append(bool(i))
    return result
