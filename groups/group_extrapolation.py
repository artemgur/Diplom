import numpy as np
from scipy.interpolate import RegularGridInterpolator

from aggregate_initializer import AggregateInitializer
from groups.group_forecast import GroupForecast

extrapolation_method_min_points = {'linear': 0, 'slinear': 0, 'cubic': 4, 'quintic': 6, 'pchip': 4}


class GroupExtrapolation(GroupForecast):
    def __init__(self, agg_list_initializer: list[AggregateInitializer], append_only=False, cache_size=10, extrapolation_method='linear'):
        super().__init__(agg_list_initializer=agg_list_initializer, append_only=append_only, cache_size=cache_size)
        self._extrapolation_method = extrapolation_method

        self._aggregates_count = len(self._aggregate_list)

        self._padding = max(extrapolation_method_min_points[extrapolation_method] - self._aggregates_count, 0)

        self._columns_index = list(np.arange(0, self._aggregates_count + self._padding))


    def _update_cache(self):
        super()._update_cache()
        self._aggregate_cache[-1] = self._aggregate_cache[-1] + [0] * self._padding



    def _forecast(self, forecast_timestamp):

        # If not enough values in history for extrapolation
        if len(self._aggregate_cache) < extrapolation_method_min_points[self._extrapolation_method]:
            return self._aggregate_cache[-1][:self._aggregates_count]

        # self._aggregate_cache and/or self._aggregate_cache_timestamps probably should be converted from deque to something else?
        #print(self._aggregate_cache)
        interp = RegularGridInterpolator([self._aggregate_cache_timestamps, self._columns_index], self._aggregate_cache,
                                         method=self._extrapolation_method, bounds_error=False, fill_value=None)

        points_to_calculate = list(map(lambda x: (forecast_timestamp, x), self._columns_index))
        extrapolated_row: np.ndarray = interp(points_to_calculate)
        return extrapolated_row.tolist()[:self._aggregates_count]
