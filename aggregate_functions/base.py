#from typing import Callable

from .utilities import empty_function


class Aggregate:
    def __init__(self, state_function, **kwargs):
        self._state_function = state_function
        self._final_function = kwargs['final_function']
        self._state = kwargs['init_state']
        # TODO deal with None/NULL
        # TODO typing
        # TODO replace kwargs with normal arguments when API will finalize

    def add_value(self, value):
        self._state = self._state_function(self._state, value)

    def get_result(self):
        return self._final_function(self._state)

    @property
    def state_function(self):
        return self._state_function

    @property
    def final_function(self):
        return self._final_function

    @property
    def state(self):
        return self.state