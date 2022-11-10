from . import Aggregate


class Average(Aggregate):
    def __init__(self, state=(0, 0)):
        super().__init__(state=state)

    def add_value(self, value):
        self._state = self._state[0] + value, self._state[1] + 1

    def remove_value(self, value):
        self._state = self._state[0] - value, self._state[1] - 1

    def get_result(self):
        return self._state[0] / self._state[1]

    # TODO remove_value
