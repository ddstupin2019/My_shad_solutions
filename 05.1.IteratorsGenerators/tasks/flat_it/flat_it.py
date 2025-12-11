from collections.abc import Iterable, Iterator
from typing import Any


def flat_it(sequence: Iterable[Any]) -> Iterator[Any]:
    """
    :param sequence: iterable with arbitrary level of nested iterables
    :return: generator producing flatten sequence
    """
    for i in sequence:
        try:
            if type(i) is str and len(i)==1:
                raise TypeError
            yield from (e for e in flat_it(i))
        except TypeError:
            yield i
