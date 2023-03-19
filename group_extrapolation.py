import time
from collections import deque

import numpy as np
from scipy.interpolate import RegularGridInterpolator

from aggregate_initializer import AggregateInitializer
from group import Group


extrapolation_method_min_points = {'linear': 0, 'cubic': 4, 'quintic': 6, 'pchip': 4}


# TODO better name
class GroupExtrapolation(Group):
    def __init__(self, agg_list_initializer: list[AggregateInitializer], append_only=False, cache_size=100, extrapolation_method='linear'):
        super().__init__(agg_list_initializer, append_only=append_only)
        self._aggregate_cache = deque()
        self._aggregate_cache_timestamps = deque()
        self._cache_size = cache_size
        self._extrapolation_method = extrapolation_method

        self._aggregates_count = len(self._aggregate_list)

        self._padding = max(extrapolation_method_min_points[extrapolation_method] - self._aggregates_count, 0)

        self._columns_index = list(np.arange(0, self._aggregates_count + self._padding))


    def insert(self, row):
        super().insert(row)
        self._update_cache()


    def delete(self, row):
        delete_actually_removed_row = super().delete(row)
        if delete_actually_removed_row:
            self._update_cache()


    def _update_cache(self):
        aggregate_values = list(map(lambda x: x.aggregate.get_result(), self._aggregate_list)) + [0] * self._padding
        self._aggregate_cache.append(aggregate_values)
        self._aggregate_cache_timestamps.append(time.time())
        if len(self._aggregate_cache) > self._cache_size:
            self._aggregate_cache.popleft()
            self._aggregate_cache_timestamps.popleft()


    def extrapolate(self, extrapolation_timestamp=None):
        if extrapolation_timestamp is None:
            extrapolation_timestamp = time.time()

        # If not enough values in history for extrapolation
        if len(self._aggregate_cache) < extrapolation_method_min_points[self._extrapolation_method]:
            return self._aggregate_cache[-1][:self._aggregates_count]

        # self._aggregate_cache and/or self._aggregate_cache_timestamps probably should be converted from deque to something else?
        #print(self._aggregate_cache)
        interp = RegularGridInterpolator([self._aggregate_cache_timestamps, self._columns_index], self._aggregate_cache,
                                         method=self._extrapolation_method, bounds_error=False, fill_value=None)

        points_to_calculate = list(map(lambda x: (extrapolation_timestamp, x), self._columns_index))
        extrapolated_row: np.ndarray = interp(points_to_calculate)
        return extrapolated_row.tolist()[:self._aggregates_count]
