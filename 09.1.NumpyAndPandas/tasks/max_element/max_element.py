import numpy as np
import numpy.typing as npt


def max_element(array: npt.NDArray[np.int_]) -> int | None:
    """
    Return max element before zero for input array.
    If appropriate elements are absent, then return None
    :param array: array,
    :return: max element value or None
    """
    shift_array = np.roll(array, 1)
    shift_array[0] = 1
    return None if array[shift_array == 0].size == 0 else int(np.max(array[shift_array == 0]))
