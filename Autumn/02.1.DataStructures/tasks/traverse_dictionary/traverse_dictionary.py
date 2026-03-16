import typing as tp


def traverse_dictionary_immutable(
        dct: tp.Mapping[str, tp.Any],
        prefix: str = "") -> list[tuple[str, int]]:
    """
    :param dct: dictionary of undefined depth with integers or other dicts as leaves with same properties
    :param prefix: prefix for key used for passing total path through recursion
    :return: list with pairs: (full key from root to leaf joined by ".", value)
    """
    r = []
    for i, j in dct.items():
        if isinstance(j, int):
            r.append((prefix + i, j))
        else:
            r += traverse_dictionary_immutable(j, prefix + i + '.')
    return r


def traverse_dictionary_mutable(
        dct: tp.Mapping[str, tp.Any],
        result: list[tuple[str, int]],
        prefix: str = "") -> None:
    """
    :param dct: dictionary of undefined depth with integers or other dicts as leaves with same properties
    :param result: list with pairs: (full key from root to leaf joined by ".", value)
    :param prefix: prefix for key used for passing total path through recursion
    :return: None
    """
    for i, j in dct.items():
        if isinstance(j, int):
            result.append((prefix + i, j))
        else:
            traverse_dictionary_mutable(j, result, prefix + i + '.')


def traverse_dictionary_iterative(
        dct: tp.Mapping[str, tp.Any]
) -> list[tuple[str, int]]:
    """
    :param dct: dictionary of undefined depth with integers or other dicts as leaves with same properties
    :return: list with pairs: (full key from root to leaf joined by ".", value)
    """
    r = []
    stac = [(dct,'')]

    while stac:
        dct, prefix = stac.pop(-1)
        for i,j in dct.items():
            if isinstance(j, int):
                r.append((prefix + i, j))
            else:
                stac.append((j, prefix+i+'.'))
    return r
