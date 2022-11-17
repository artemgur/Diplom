from . import Aggregate
from .utilities.countable_bloom_filter import CountableBloomFilter
from .utilities import hash_functions


class CountDistinctCBF(Aggregate):
    def __init__(self, state=None, expected_element_count=100, false_positive_probability=0.01):
        if state is not None:  # TODO do something better about state?
            raise ValueError('state parameter is not supported in CountDistinctCBF')
        super().__init__(state=state)
        self._state = CountableBloomFilter.create(expected_element_count, false_positive_probability, hash_functions.sha256, hash_functions.md5)



    def add_value(self, value):
        self._state.add(value)


    def get_result(self):
        return self._state.elements_count


    def remove_value(self, value):
        self._state.remove(value)


    def _reset_state(self):
        raise NotImplementedError


