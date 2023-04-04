from collections import defaultdict

from . import Aggregate


class CountDistinct(Aggregate):
    def __init__(self, state=None, column_cache=None):
        if state is None:
            state = defaultdict(lambda: 0)
        super().__init__(state=state, column_cache=column_cache)

    def insert(self, value):
        self._state[value] += 1

    def delete(self, value):
        self._state[value] -= 1
        if self._state[value] == 0:
            del self._state[value]

    def get_result(self):
        return len(self._state)

    @classmethod
    def needs_column_cache(cls):
        return False

