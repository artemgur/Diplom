from collections import defaultdict

import numpy as np
from scipy.interpolate import RegularGridInterpolator

from utilities import n_values_with_step


def extrapolate_matrix(data_history, seconds_between_snapshots, seconds_before_extrapolation):
    result_matrix = np.empty_like(data_history[-1])

    for data_row in range(data_history.shape[1]):
        row_history = data_history[:, data_row, :]

        extrapolated_row = extrapolate(row_history, seconds_before_extrapolation, seconds_between_snapshots)
        result_matrix[data_row] = extrapolated_row

    return result_matrix


def extrapolate(matrix, seconds_before_extrapolation, seconds_between_snapshots):
    row_history_timestamp_index = n_values_with_step(matrix.shape[0], seconds_between_snapshots)
    columns_index = np.arange(0, matrix.shape[1])
    interp = RegularGridInterpolator([row_history_timestamp_index, columns_index], matrix,
                                     method='linear', bounds_error=False, fill_value=None)
    extrapolation_timestamp = row_history_timestamp_index[-1] + seconds_before_extrapolation
    points_to_calculate = list(map(lambda x: (extrapolation_timestamp, x), columns_index))
    extrapolated_row = interp(points_to_calculate)
    return extrapolated_row




