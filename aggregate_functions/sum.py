from . import Aggregate


class Sum(Aggregate):
    def __init__(self, state=0):
        super().__init__(state=state)

    def add_value(self, value):
        self._state += value

    def remove_value(self, value):
        self._state -= value
