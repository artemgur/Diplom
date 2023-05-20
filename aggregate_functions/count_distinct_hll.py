import hyperloglog

from . import Aggregate


class CountDistinctHLL(Aggregate):
    def __init__(self, state=None, column_cache=None, error_rate=0.01):
        super().__init__(state=state, column_cache=column_cache)
        if self._state is None:
            self._state = hyperloglog.HyperLogLog(error_rate=error_rate)


    def insert(self, value):
        self._state.add(value)


    def get_result(self):
        return len(self._state)
