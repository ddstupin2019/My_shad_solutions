def filter_list_by_list(lst_a: list[int] | range, lst_b: list[int] | range) -> list[int]:
    """
    Filter first sorted list by other sorted list
    :param lst_a: first sorted list
    :param lst_b: second sorted list
    :return: filtered sorted list
    """
    if not lst_b:
        return list(lst_a)
    result = []
    ind_b = 0
    for elem_a in lst_a:
        while lst_b[ind_b] < elem_a and ind_b < len(lst_b)-1:
            ind_b += 1
        if lst_b[ind_b] != elem_a:
            result.append(elem_a)
    return result
