import numpy as np
from scipy.interpolate import RegularGridInterpolator

from utilities import n_values_with_step


def extrapolate(data, seconds_between_snapshots, seconds_before_extrapolation):
    extrapolated = np.empty_like(data[-1])
    for data_row in range(data.shape[1]):
        row_history = data[:,data_row,:]
        row_history_time_index = n_values_with_step(row_history.shape[0], seconds_between_snapshots)
        columns_index = np.arange(0, row_history.shape[1])
        interp = RegularGridInterpolator([row_history_time_index, columns_index], row_history, method='linear', bounds_error=False, fill_value=None)
        extrapolation_time = row_history_time_index[-1] + seconds_before_extrapolation
        points = list(map(lambda x: (extrapolation_time, x), columns_index))
        result = interp(points)
        extrapolated[data_row] = result
    return extrapolated
