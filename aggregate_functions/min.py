from . import Aggregate


# TODO we need to do full recalculation here only if we remove value equal to max
class Min(Aggregate):
    def __init__(self, state=float('inf'), column_cache=None):  # TODO better
        super().__init__(state=state, column_cache=column_cache)


    def insert(self, value):
        self._state = min(self._state, value)

    def delete(self, value):
        if value == self._state:
            self._column_cache_full_recalculation()
