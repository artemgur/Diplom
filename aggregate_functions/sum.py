from . import Aggregate


class Sum(Aggregate):
    def __init__(self, state=0, column_cache=None):
        super().__init__(state=state, column_cache=column_cache)

    def insert(self, value):
        self._state += value

    def delete(self, value):
        self._state -= value

    @classmethod
    def needs_column_cache(cls):
        return False

