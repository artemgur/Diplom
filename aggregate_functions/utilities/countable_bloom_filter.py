from typing import TypeVar, Generic, Callable
import math

from .km_hasher import KMHasher


T = TypeVar('T')


# TODO change implementation
# Вычислять количество элементов по формуле из Википедии!


class CountableBloomFilter(Generic[T]):  # TODO rename to CountingBloomFilter?
    def __init__(self, length: int, hasher: KMHasher):
        self.__hasher = hasher
        self.__bloom_filter = [0] * length
        #self.__elements_count = 0


    @staticmethod
    def create(expected_elements_count: int, false_positive_probability: float, hash_func1, hash_func2):
        length = get_optimal_length(expected_elements_count, false_positive_probability)
        hash_functions_count = get_optimal_hash_functions_count(length, expected_elements_count)
        hasher = KMHasher(hash_functions_count, hash_func1, hash_func2)
        return CountableBloomFilter(length, hasher)


    def __hash_element(self, element: T) -> set[int]:
        return set(map(lambda x: x % len(self.__bloom_filter), self.__hasher.hash(element)))


    def add(self, element: T):  # -> bool:  # Addition is always successful, no need for a return value
        #contains_result = self.__contains(element)
        #if contains_result[0]:
        #    return False
        element_hash = self.__hash_element(element) # contains_result[1]
        #self.__elements_count += 1
        for i in element_hash:
            self.__bloom_filter[i] += 1
        #return True


    def remove(self, element: T) -> bool:
        contains_result = self.__contains(element)
        if contains_result[0]:
            return False
        element_hash = contains_result[1]
        #self.__elements_count -= 1
        for i in element_hash:
            self.__bloom_filter[i] -= 1
        return True


    def __contains(self, element: T) -> (bool, set[int]):
        element_hash = self.__hash_element(element)
        for i in element_hash:
            if self.__bloom_filter[i] == 0:
                return False
        return True, element_hash


    def contains(self, element: T):
        return self.__contains(element)[0]


    # TODO "bits" is not the most precise word here, rename?
    @property
    def non_zero_bits_count(self):
        return len(list(filter(lambda x: x > 0, self.__bloom_filter)))


    @property
    def false_positive_probability(self):
        return (1 - math.exp(-self.__hasher.hash_functions_count * self.elements_count / len(self.__bloom_filter))) ** self.__hasher.hash_functions_count


    @property
    def length(self):
        return len(self.__bloom_filter)

    @property
    def hash_functions_count(self):
        return self.__hasher.hash_functions_count

    # TODO
    @property
    def elements_count(self):
        return -self.length / self.hash_functions_count * math.log(1 - self.non_zero_bits_count / self.length)
        #return self.__elements_count


def get_optimal_hash_functions_count(length: int, expected_elements_count: int) -> int:
    return round(math.log(2) * length / expected_elements_count)


def get_optimal_length(expected_elements_count, false_positive_probability):
    #return math.ceil(expected_elements_count * math.log(false_positive_probability) / (math.log(1 / 2 ** math.log(2))))
    return math.ceil(- expected_elements_count * math.log(false_positive_probability) / (math.log(2) ** 2))
