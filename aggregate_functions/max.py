from . import Aggregate


class Max(Aggregate):
    def __init__(self, state=0):  # TODO better
        super().__init__(state=state)


    def add_value(self, value):
        self._state = max(self._state, value)
