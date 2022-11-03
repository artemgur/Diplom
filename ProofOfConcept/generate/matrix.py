import random

import numpy as np
import pandas as pd

import row_change_function as rcf
import vector


def random_matrix(shape, multiplier=1):
    return np.random.randn(shape[0], shape[1]) * multiplier


def function_matrix(shape, row_change_function=rcf.linear, multiplier=1):
    coefs = vector.random_row(shape[1])
    first_row = vector.random_row(shape[1], multiplier)
    rows = [first_row]
    for i in range(1, shape[0]):
        rows.append(row_change_function(coefs, rows[-1]))
    return np.stack(rows, axis=0)


def categorical_df(numeric_shape, multiplier=1):  #, categorical_count=3):
    numeric = function_matrix(numeric_shape, multiplier=multiplier)
    df = pd.DataFrame(numeric)  # TODO column names?
    random_column = df.columns[random.randint(0, numeric_shape[1] - 1)]
    df[f'cat_corr_{random_column}'] = vector.categorical_correlated(df[random_column])
    df['cat_no_corr'] = vector.categorical_non_correlated(numeric_shape[0])
    return df


#print(categorical_df((40, 4), 10))

