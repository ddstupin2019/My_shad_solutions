import typing as tp


def revert(dct: tp.Mapping[str, str]) -> dict[str, list[str]]:
    """
    :param dct: dictionary to revert in format {key: value}
    :return: reverted dictionary {value: [key1, key2, key3]}
    """
    r = {}
    for value, key in dct.items():
        if key in r:
            r[key].append(value)
        else:
            r[key] = [value]
    return r
