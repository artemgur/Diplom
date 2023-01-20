import pandas as pd
from scipy.stats import f_oneway

import pandas_utilities as pdu


class CategoryEncoder:
    def __init__(self, p_value_threshold=0.1):
        self._p_value_threshold = p_value_threshold
        self._best_correlations = {}  # Словарь пока не используется, но может пригодиться
        self._best_correlations_means = {}
        self._not_correlated_columns = []

    # TODO новые значения
    def fit(self, data: pd.DataFrame):
        categorical_columns = pdu.get_categorical_column_names(data)
        numerical_columns = pdu.get_numerical_column_names(data)

        #best_correlations = {}
        #best_correlations_means = {}

        for categorical_column in categorical_columns:
            local_columns = [*numerical_columns, categorical_column]
            local_data: pd.DataFrame = data[local_columns]
            groupby = local_data.groupby(categorical_column)

            best_p_value = 10
            best_p_value_column = None

            for numerical_column in numerical_columns:
                groupby_list = groupby[numerical_column].apply(list)
                #print(groupby_list)
                p_value = f_oneway(*groupby_list)[1]
                if p_value < best_p_value:
                    best_p_value = p_value
                    best_p_value_column = numerical_column

            best_mean = groupby[best_p_value_column].mean().to_dict()
            #print(best_mean)

            if best_p_value < self._p_value_threshold:
                self._best_correlations[categorical_column] = best_p_value_column
                self._best_correlations_means[categorical_column] = best_mean
            else:
                self._not_correlated_columns.append(categorical_column)


        return self
        # return best_correlations_means
        # return best_correlations, best_correlations_means

    def encode(self, data: pd.DataFrame):
        result = pdu.get_numerical_columns(data)
        for categorical_column in self._best_correlations_means:  # Ключи словаря
            result[categorical_column] = data[categorical_column].replace(self._best_correlations_means[categorical_column])
        return result

    def decode(self, data: pd.DataFrame):
        result = pdu.get_numerical_columns(data)
        for categorical_column in self._best_correlations_means:  # Ключи словаря
            # Finds key-value pair where value is closest to encoded number, and then takes the key of pair. Can be improved later
            result[categorical_column] = data[categorical_column].apply(
                lambda x: min(self._best_correlations_means[categorical_column].items(), key=lambda y: abs(x - y[1]))[0])
        return result

    @property
    def not_correlated_columns(self):
        return self._not_correlated_columns
