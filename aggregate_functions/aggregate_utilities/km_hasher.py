# https://stackoverflow.com/a/40343867
from typing import Callable


class KMHasher:
    def __init__(self, hash_func_count, hash_func1: Callable, hash_func2: Callable):
        if hash_func_count < 1:
            raise ValueError('Hash functions count must be at least 1')

        self.__hash_func1 = hash_func1
        self.__hash_func2 = hash_func2
        self.__hash_func_count = hash_func_count


    def hash(self, element):
        hash1 = self.__hash_func1(element)

        hash2 = self.__hash_func2(element)

        for i in range(self.__hash_func_count - 2):
            yield hash1 + i * hash2

    @property
    def hash_functions_count(self):
        return self.__hash_func_count

