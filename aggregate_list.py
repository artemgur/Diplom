from typing import Dict

from aggregate_functions import Aggregate


class AggregateList:
    # agg_list_initializer: dict, key – column name, value – lambda/function, which initializes aggregate
    def __init__(self, agg_list_initializer):
        self._aggregate_list: Dict[str, Aggregate] = {key: value() for key, value in agg_list_initializer.items()}

    def add_row(self, row):
        for column_name in self._aggregate_list:  # Iterates over keys
            self._aggregate_list[column_name].add_value(row[column_name])

    def remove_row(self, row):
        for column_name in self._aggregate_list:  # Iterates over keys
            self._aggregate_list[column_name].remove_value(row[column_name])

    def get_result(self):
        return list(map(lambda key: self._aggregate_list[key].get_result(), self._aggregate_list))