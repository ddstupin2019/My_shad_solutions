def normalize_path(path: str) -> str:
    """
    :param path: unix path to normalize
    :return: normalized path
    """
    if path == '.' or path == '':
        return '.'
    inp_list = path.split('/')
    result = []
    is_str = False
    for i in inp_list:
        if i == '':
            continue
        elif i == '.':
            if len(result) == 0:
                result.append(i)
        elif i == '..':
            if len(result) == 0:
                result.append(i)
            elif result[-1] == '.' or result[-1] == '..':
                result.append(i)
            else:
                result.pop()
        else:
            is_str = True
            result.append(i)

    if len(result) == 0:
        if path[0] == '/':
            return '/'
        return '.'

    if result[0] == '.':
        result.pop(0)
    if path[0] == '/':
        if not is_str:
            return '/'
        else:
            while result[0] == '..':
                result.pop(0)
            return '/' + '/'.join(result)
    return '/'.join(result)
