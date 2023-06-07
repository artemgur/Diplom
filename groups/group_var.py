import itertools
from collections import deque

import numpy as np
from scipy.interpolate import RegularGridInterpolator
from statsmodels.tsa.api import VAR
from statsmodels.tsa.stattools import adfuller

from aggregate_initializer import AggregateInitializer
from groups.group_forecast import GroupForecast


extrapolation_method_min_points = {'linear': 0, 'slinear': 0, 'cubic': 4, 'quintic': 6, 'pchip': 4}


# TODO better name
class GroupVAR(GroupForecast):
    def __init__(self, agg_list_initializer: list[AggregateInitializer], append_only=False, cache_size=10, extrapolation_method='linear', max_lags=1):
        super().__init__(agg_list_initializer=agg_list_initializer, append_only=append_only, cache_size=cache_size)
        self._extrapolation_method = extrapolation_method

        self._aggregates_count = len(self._aggregate_list)

        self._padding = max(extrapolation_method_min_points[extrapolation_method] - self._aggregates_count, 0)

        self._columns_index = list(np.arange(0, self._aggregates_count + self._padding))

        self._max_lags = max_lags


    def _update_cache(self):
        super()._update_cache()
        self._aggregate_cache[-1] = self._aggregate_cache[-1] + [0] * self._padding


    def _forecast(self, forecast_timestamp):
        if len(self._aggregate_cache) < extrapolation_method_min_points[self._extrapolation_method]:
            return self._aggregate_cache[-1][:self._aggregates_count]

        evenly_spaced_points = self._to_evenly_spaced(forecast_timestamp)
        #for column_number in range(evenly_spaced_points.shape[1]):
        #   column_history = evenly_spaced_points[:, column_number]
        #   if adfuller(column_history)[1] <= 0.05:
        #       print(column_number)
        #       evenly_spaced_points[:, column_number] = np.diff(column_history, axis=0)

        model = VAR(evenly_spaced_points)
        results = model.fit(self._max_lags)

        prediction: np.ndarray = results.forecast(evenly_spaced_points, steps=1)
        return prediction[0].tolist()


    def _to_evenly_spaced(self, forecast_timestamp: int):
        #print(self._aggregate_cache)
        delta = forecast_timestamp - self._aggregate_cache_timestamps[-1]

        interp = RegularGridInterpolator([self._aggregate_cache_timestamps, self._columns_index], self._aggregate_cache,
                                         method=self._extrapolation_method, bounds_error=False, fill_value=None)

        evenly_spaced_timestamps = self._evenly_spaced_timestamps(delta)
        points_to_calculate = list(itertools.product(evenly_spaced_timestamps, np.arange(0, self._aggregates_count)))
        interpolated_values: np.ndarray = interp(points_to_calculate)
        interpolated_points = np.array(np.split(interpolated_values, len(evenly_spaced_timestamps)))
        return interpolated_points


    def _evenly_spaced_timestamps(self, delta: int):
        cache_interval_size = self._aggregate_cache_timestamps[-1] - self._aggregate_cache_timestamps[0]
        points_count = int(cache_interval_size // delta)
        points = deque()
        for i in range(points_count):
            points.appendleft(self._aggregate_cache_timestamps[-1] - i * delta)
        return points

    # def evenly_spaced_timestamps(self):
    #     points_count = len(self._aggregate_cache_timestamps)
    #     points_interval = (self._aggregate_cache_timestamps[-1] - self._aggregate_cache_timestamps[0]) / (points_count - 1)
    #     points = deque()
    #     for i in range(points_count):
    #         points.appendleft(self._aggregate_cache_timestamps[-1] - i * points_interval)
    #     return points