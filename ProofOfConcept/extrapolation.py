import numpy as np
from scipy.interpolate import RegularGridInterpolator

from utilities import n_values_with_step


def extrapolate(data, seconds_between_snapshots, seconds_before_extrapolation):
    extrapolated = np.empty_like(data[-1])
    for data_row in range(data.shape[1]):
        row_history = data[:,data_row,:]

        row_history_timestamp_index = n_values_with_step(row_history.shape[0], seconds_between_snapshots)
        columns_index = np.arange(0, row_history.shape[1])

        interp = RegularGridInterpolator([row_history_timestamp_index, columns_index], row_history, method='linear', bounds_error=False, fill_value=None)

        extrapolation_timestamp = row_history_timestamp_index[-1] + seconds_before_extrapolation
        points_to_calculate = list(map(lambda x: (extrapolation_timestamp, x), columns_index))

        result = interp(points_to_calculate)
        extrapolated[data_row] = result
    return extrapolated
