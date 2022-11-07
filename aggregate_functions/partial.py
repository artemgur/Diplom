from .base import Aggregate


class PartialAggregate(Aggregate):
    def __init__(self, state_function, **kwargs):
        super().__init__(state_function, **kwargs)
        self._combine_function = kwargs['combine_function']

    def combine(self, other_partial_state):
        self._state = self._combine_function(self._state, other_partial_state)
