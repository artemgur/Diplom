from collections import defaultdict
import random

import numpy as np
import pandas as pd

from . import utilities


def random_row(length, multiplier=10):
    return np.random.randn(length) * multiplier


def categorical_non_correlated(rows, categorical_size=5):
    categories = utilities.get_char_list(categorical_size)
    transition_probabilities = defaultdict(lambda: [1] * categorical_size)
    transition_probabilities['a'] = [4, 3] + [1] * (categorical_size - 2)
    result = [random.choice(categories)]
    for i in range(1, rows):
        result.append(random.choices(categories, weights=transition_probabilities[result[i - 1]])[0])
    return result


def categorical_correlated(correlated_column: pd.Series, categorical_size=5):
    categories = utilities.get_char_list(categorical_size)
    return pd.cut(correlated_column, categorical_size, labels=categories)


