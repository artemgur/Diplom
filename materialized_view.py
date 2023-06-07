from collections import defaultdict
from typing import Iterable

from tabulate import tabulate

import constants
from aggregate_initializer import AggregateInitializer
from groups.group import Group
from groups import *
from groups.group_extrapolation import GroupExtrapolation
#from group_var import GroupVAR as GroupExtrapolation
from orderby import OrderBy
from utilities.empty_functions import empty_where_function
import utilities.list


# TODO optional ordered storage?
# It should be easy to make ordered storage based on groupby column, but harder – based on aggregate
class MaterializedView:
    # groupby_columns – list[str] (or maybe tuple[str]) of column names
    # _groupby_rows – dict. Key – tuple of values of groupby columns, values – list of aggregate functions
    def __init__(self, name: str, groupby_columns: list[str], aggregate_initializers: list[AggregateInitializer],
                 where=empty_where_function,
                 append_only=False,
                 column_aliases=None,
                 forecast=False, forecast_type=GroupExtrapolation, extrapolation_method='linear', extrapolation_cache_size=100):

        if column_aliases is None:
            column_aliases = []
        self._name = name


        self._aggregate_initializers = aggregate_initializers
        self._append_only = append_only
        self._extrapolation = forecast
        self._extrapolation_method = extrapolation_method
        self._extrapolation_cache_size = extrapolation_cache_size
        self._groupby_rows = defaultdict(lambda: Group(aggregate_initializers, append_only=append_only)) if not self._extrapolation else \
            defaultdict(lambda: forecast_type(aggregate_initializers, append_only=append_only,
                                              extrapolation_method=self._extrapolation_method, cache_size=self._extrapolation_cache_size))
        self._groupby_columns = groupby_columns
        self._where = where
        self._column_aliases = self._determine_column_aliases(column_aliases)

    # row is dict for now
    def insert(self, row):
        if not self._where(**row):
            return False

        groupby_values = tuple(row[key] for key in self._groupby_columns)
        self._groupby_rows[groupby_values].insert(row)
        return True

    def delete(self, row):
        if self.append_only:
            raise ValueError(f'Attempted to delete an item from append-only materialized view {self.name}')

        groupby_values = tuple(row[key] for key in self._groupby_columns)
        self._groupby_rows[groupby_values].delete(row)
        return True


    def update(self, old_row, new_row):
        if self.append_only:
            raise ValueError(f'Attempted to update an item in append-only materialized view {self.name}')

        self.delete(old_row)
        self.insert(new_row)


    @property
    def _base_column_names(self):
        return self._groupby_columns + list(map(lambda x: str(x), self._aggregate_initializers))


    def get_rows(self):
        for key, value in self._groupby_rows.items():
            yield list(key) + value.get_result()


    def _forecast(self, extrapolation_timestamp=None, extrapolation_offset=None):
        if not self._extrapolation:
            raise ValueError('Attempted to forecast a groupby in which forecasting is not enabled')
        for key, value in self._groupby_rows.items():
            # noinspection PyUnresolvedReferences
            yield list(key) + value.forecast(forecast_timestamp=extrapolation_timestamp, forecast_offset=extrapolation_offset)

    def to_string_forecasted(self, extrapolation_timestamp=None):
        return tabulate(self._forecast(extrapolation_timestamp), headers=self.column_names, tablefmt=constants.TABULATE_FORMAT)


    @property
    def name(self):
        return self._name

    @property
    def columns_count(self):
        return len(self._groupby_columns) + len(self._aggregate_initializers)

    @property
    def column_names(self):
        return self._column_aliases

    @property
    def append_only(self):
        return self._append_only


    def _determine_column_aliases(self, column_aliases: list[str]):
        # Padding column_aliases list to columns count
        column_names = column_aliases + [''] * (self.columns_count - len(column_aliases))
        for i in range(len(column_names)):
            if column_names[i] == '':
                column_names[i] = self._base_column_names[i]
        return column_names


    def _select(self, input_rows, column_names=None, where=empty_where_function) -> Iterable:
        #if column_names is None:
        #    column_names = self.column_names
        where_rows = filter(lambda x: where(*x), input_rows)
        if column_names is None:
            return where_rows
        column_indexes = utilities.list.find_multiple(self.column_names, column_names)
        rows = map(lambda x: utilities.list.index_many(x, column_indexes), where_rows)
        return rows


    def orderby(self, column_names: list[str], rows: Iterable, orderby_list: list[OrderBy]) -> list:
        if column_names is None:
            column_names = self.column_names

        rows_list = list(rows)
        for orderby in reversed(orderby_list):
            index_to_sort = column_names.index(orderby.column_name)
            rows_list.sort(key=lambda x: x[index_to_sort], reverse=orderby.desc)
        return rows_list



    def select(self, column_names=None, where=empty_where_function) -> Iterable:
        return self._select(self.get_rows(), column_names, where)

    def select_forecasted(self, column_names=None, where=empty_where_function, forecast_timestamp=None, forecast_offset=None) -> Iterable:
        rows = self._forecast(extrapolation_timestamp=forecast_timestamp, extrapolation_offset=forecast_offset)
        return self._select(rows, column_names, where)


    def __str__(self):
        return tabulate(self.get_rows(), headers=self.column_names, tablefmt=constants.TABULATE_FORMAT)


    # To store materialized views in sets
    def __eq__(self, other):
        return self._name == other.name


    # To store materialized views in sets
    def __hash__(self):
        return hash(self._name)
