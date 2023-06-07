import time
from abc import ABC, abstractmethod
from collections import deque

import numpy as np
from scipy.interpolate import RegularGridInterpolator

from aggregate_initializer import AggregateInitializer
from groups.group import Group


class GroupForecast(Group, ABC):
    def __init__(self, agg_list_initializer: list[AggregateInitializer], append_only=False, cache_size=10):
        super().__init__(agg_list_initializer, append_only=append_only)
        self._aggregate_cache = deque()
        self._aggregate_cache_timestamps = deque()
        self._cache_size = cache_size


    def insert(self, row):
        super().insert(row)
        self._update_cache()


    def delete(self, row):
        delete_actually_removed_row = super().delete(row)
        if delete_actually_removed_row:
            self._update_cache()


    def _update_cache(self):
        aggregate_values = list(map(lambda x: x.aggregate.get_result(), self._aggregate_list))

        current_time = time.time()
        if len(self._aggregate_cache_timestamps) > 0 and self._aggregate_cache_timestamps[-1] == current_time:
            self._aggregate_cache.pop()
            self._aggregate_cache_timestamps.pop()

        self._aggregate_cache.append(aggregate_values)
        self._aggregate_cache_timestamps.append(current_time)
        if len(self._aggregate_cache) > self._cache_size:
            self._aggregate_cache.popleft()
            self._aggregate_cache_timestamps.popleft()


    def forecast(self, forecast_timestamp=None, forecast_offset=None):
        if forecast_offset is not None:
            forecast_timestamp = self._aggregate_cache_timestamps[-1] + forecast_offset
        if forecast_timestamp is None:
            forecast_timestamp = time.time()

        return self._forecast(forecast_timestamp)


    @abstractmethod
    def _forecast(self, forecast_timestamp):
        ...
