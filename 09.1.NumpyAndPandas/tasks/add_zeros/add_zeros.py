import numpy as np
import numpy.typing as npt


def add_zeros(x: npt.NDArray[np.int_]) -> npt.NDArray[np.int_]:
    """
    Add zeros between values of given array
    :param x: array,
    :return: array with zeros inserted
    """
    result = np.zeros(max(0, x.size * 2 - 1), dtype=x.dtype)
    result[::2] = x
    return result
