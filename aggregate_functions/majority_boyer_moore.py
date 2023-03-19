from . import Aggregate


class MajorityBM(Aggregate):
    def __init__(self, state=None, column_cache=None):
        super().__init__(state=state, column_cache=column_cache)
        self._count = 0


    def insert(self, value):
        if self._count == 0 and self._state != value:
            self._state = value
            self._count = 1
        elif self._state == value:
            self._count += 1
        else:
            self._count -= 1
