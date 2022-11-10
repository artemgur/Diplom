from typing import Iterable


class Aggregate:
    def __init__(self, state=None, row_cache=None):
        self._initial_state = state  # TODO init this variable only if it will be actually used later?
        self._state = state
        self._row_cache = row_cache  # TODO implement row cache, update usages after that

    def add_value(self, value):
        raise NotImplementedError

    def get_result(self):
        return self._state

    def remove_value(self, value):
        raise NotImplementedError

    def combine(self, partial_state):
        raise NotImplementedError

    def add_values(self, values: Iterable):
        for value in values:
            self.add_value(value)

    @property
    def state(self):
        return self.state

    def _reset_state(self):
        self._state = self._initial_state

    def _row_cache_full_recalculation(self):
        # TODO remove value from row cache here or assume it was already removed?
        self._reset_state()
        self.add_values(self._row_cache)

