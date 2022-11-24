from typing import Iterable

from utilities import utilities


class Aggregate:
    # TODO mirror constructor arguments changes to subclasses
    def __init__(self, state=None, row_cache=None):
        self._initial_state = state  # TODO init this variable only if it will be actually used later?
        self._state = state
        self._row_cache = row_cache  # TODO implement row cache, update usages after that

    def add_value(self, value):
        raise NotImplementedError

    def get_result(self):
        return self._state

    def remove_value(self, value):
        raise NotImplementedError

    def combine(self, partial_state):
        raise NotImplementedError

    def add_values(self, values: Iterable):
        for value in values:
            self.add_value(value)

    @property
    def state(self):
        return self.state

    def _reset_state(self):
        self._state = self._initial_state

    def _row_cache_full_recalculation(self):
        # TODO remove value from row cache here or assume it was already removed?
        self._reset_state()
        self.add_values(self._row_cache)

    @classmethod
    def has_delete(cls):
        return not cls.remove_value == Aggregate.remove_value

    @classmethod
    def has_combine(cls):
        return not cls.combine == Aggregate.combine


    # By default, function name is the class name converted to snake_case
    # Can be overriden in subclasses to change the function name
    @classmethod
    def function_name(cls):
        return utilities.classname_to_snake_case(cls)

    # Needed to make "abstract" subclasses in hierarchy
    # TODO but base class should be "abstract", but is_function returns True for it!
    # @classmethod
    # def is_function(cls):
    #     return True

