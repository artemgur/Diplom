import numpy as np

from .matrix import random_matrix
from . import row_change_function as rcf






def generate_next(previous_data, description, row_change_function):
    next_ = np.empty_like(previous_data[-1])
    for row in range(next_.shape[0]):
        next_[row] = row_change_function(description[row], previous_data[-1][row])
    previous_data.append(next_)


def generate(count, shape, multiplier=10, row_change_function=rcf.linear):
    start = random_matrix(shape, multiplier)
    description = random_matrix(shape)
    data = [start]
    for i in range(count - 1):
        generate_next(data, description, row_change_function)
    return np.stack(data)  #, description


def add_categorical_dependent_column(data_generated):
    ...