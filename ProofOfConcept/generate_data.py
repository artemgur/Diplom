import numpy as np


def row_change_function_linear_simple(data: np.ndarray, coefs: np.ndarray):
    return data + coefs


def generate_random_matrix(rows_count, columns_count, multiplier=1):
    return np.random.randn(rows_count, columns_count) * multiplier


def generate_next(previous_data, description, row_change_function):
    next_ = np.empty_like(previous_data[-1])
    for row in range(next_.shape[0]):
        next_[row] = row_change_function(previous_data[-1][row], description[row])
    previous_data.append(next_)


def generate(count, rows_count, columns_count, multiplier=1, row_change_function = row_change_function_linear_simple):
    start = generate_random_matrix(rows_count, columns_count, multiplier)
    description = generate_random_matrix(rows_count, columns_count)
    data = [start]
    for i in range(count - 1):
        generate_next(data, description, row_change_function)
    return np.stack(data), description
