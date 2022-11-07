from collections import defaultdict

import numpy as np
import pandas as pd
from scipy.interpolate import RegularGridInterpolator

from category_encoder import CategoryEncoder
from category_extrapolator import CategoryExtrapolator
from utilities import n_values_with_step


def extrapolate_matrix_history(data_history, seconds_between_snapshots, seconds_before_extrapolation):
    result_matrix = np.empty_like(data_history[-1])

    for data_row in range(data_history.shape[1]):
        row_history = data_history[:, data_row, :]

        extrapolated_row = extrapolate_matrix(row_history, seconds_before_extrapolation, seconds_between_snapshots)
        result_matrix[data_row] = extrapolated_row

    return result_matrix


def extrapolate_matrix(matrix, seconds_before_extrapolation, seconds_between_snapshots):
    row_history_timestamp_index = n_values_with_step(matrix.shape[0], seconds_between_snapshots)
    columns_index = np.arange(0, matrix.shape[1])
    interp = RegularGridInterpolator([row_history_timestamp_index, columns_index], matrix,
                                     method='linear', bounds_error=False, fill_value=None)
    extrapolation_timestamp = row_history_timestamp_index[-1] + seconds_before_extrapolation
    points_to_calculate = list(map(lambda x: (extrapolation_timestamp, x), columns_index))
    extrapolated_row = interp(points_to_calculate)
    return extrapolated_row


def extrapolate_categorical_matrix(matrix: pd.DataFrame, seconds_before_extrapolation, seconds_between_snapshots):
    cat_encoder = CategoryEncoder()
    cat_encoder_result = cat_encoder.fit(matrix).encode(matrix)
    cat_encoder_extrapolation = extrapolate_matrix(cat_encoder_result.values, seconds_between_snapshots, seconds_before_extrapolation)
    #print(cat_encoder_extrapolation)
    cat_encoder_extrapolation_df = pd.DataFrame(cat_encoder_extrapolation[None, :], columns=list(cat_encoder_result.columns))
    cat_encoder_extrapolation_decoded = cat_encoder.decode(cat_encoder_extrapolation_df)

    non_correlated_columns = cat_encoder.not_correlated_columns
    non_correlated_extrapolations = []
    for non_correlated_column in non_correlated_columns:
        cat_extrapolator = CategoryExtrapolator(seconds_between_snapshots)
        extrapolation = cat_extrapolator.fit(matrix[non_correlated_column]).extrapolate(matrix[non_correlated_column].iloc[-1], seconds_before_extrapolation)
        non_correlated_extrapolations.append(extrapolation)
    non_correlated_extrapolations_df = pd.DataFrame(non_correlated_extrapolations, columns=non_correlated_columns)
    extrapolation_result = pd.concat((cat_encoder_extrapolation_decoded, non_correlated_extrapolations_df), axis=1)
    return extrapolation_result
    #return pd.DataFrame(cat_encoder_extrapolation_decoded.append(non_correlated_extrapolations),
    #                    columns=list(cat_encoder_extrapolation_decoded.columns).extend(non_correlated_columns))



