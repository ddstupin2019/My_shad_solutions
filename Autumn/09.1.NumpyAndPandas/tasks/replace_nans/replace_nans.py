import numpy as np
import numpy.typing as npt


def replace_nans(matrix: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    """
    Replace all nans in matrix with average of other values.
    If all values are nans, then return zero matrix of the same size.
    :param matrix: matrix,
    :return: replaced matrix
    """
    mean_val = np.nanmean(matrix)
    if  np.isnan(mean_val):
        mean_val = 0.0

    return np.nan_to_num(matrix, nan=float(mean_val))
