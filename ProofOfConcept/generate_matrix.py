import numpy as np


def generate_random_matrix(rows_count, columns_count, multiplier=1):
    return np.random.randn(rows_count, columns_count) * multiplier

def generate_categorical_df(rows_count, numeric_columns_count):
    ...
