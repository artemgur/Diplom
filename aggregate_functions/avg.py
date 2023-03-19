from . import Aggregate


class Avg(Aggregate):
    def __init__(self, state=(0, 0), column_cache=None):
        super().__init__(state=state, column_cache=column_cache)

    def insert(self, value):
        self._state = self._state[0] + value, self._state[1] + 1

    def delete(self, value):
        self._state = self._state[0] - value, self._state[1] - 1

    def get_result(self):
        if self._state[1] == 0:
            return 0  # Maybe None or NaN?
        return self._state[0] / self._state[1]

    @classmethod
    def needs_column_cache(cls):
        return False




    # TODO remove_value
