def merge_iterative(lst_a: list[int], lst_b: list[int]) -> list[int]:
    """
    Merge two sorted lists in one sorted list
    :param lst_a: first sorted list
    :param lst_b: second sorted list
    :return: merged sorted list
    """
    if not lst_b:
        return lst_a
    if not lst_a:
        return lst_b
    result = []
    ind_a = 0
    ind_b = 0
    while ind_a + ind_b < len(lst_a) + len(lst_b):
        if lst_a[ind_a] < lst_b[ind_b]:
            result.append(lst_a[ind_a])
            ind_a += 1
            if ind_a == len(lst_a):
                while ind_b < len(lst_b):
                    result.append(lst_b[ind_b])
                    ind_b += 1
                break
        else:
            result.append(lst_b[ind_b])
            ind_b += 1
            if ind_b == len(lst_b):
                while ind_a < len(lst_a):
                    result.append(lst_a[ind_a])
                    ind_a += 1
                break

    return result


def merge_sorted(lst_a: list[int], lst_b: list[int]) -> list[int]:
    """
    Merge two sorted lists in one sorted list using `sorted`
    :param lst_a: first sorted list
    :param lst_b: second sorted list
    :return: merged sorted list
    """
    return sorted(lst_a + lst_b)
