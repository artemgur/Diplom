import time
from collections import defaultdict

from tabulate import tabulate

import constants
from aggregate_initializer import AggregateInitializer
from group import Group
from group_extrapolation import GroupExtrapolation
from utilities.empty_functions import empty_where_function
import utilities.list


# TODO optional ordered storage?
# It should be easy to make ordered storage based on groupby column, but harder – based on aggregate
class Groupby:
    # groupby_columns – list[str] (or maybe tuple[str]) of column names
    # _groupby_rows – dict. Key – tuple of values of groupby columns, values – list of aggregate functions
    def __init__(self, name: str, groupby_columns: list[str], aggregate_initializers: list[AggregateInitializer], where=empty_where_function,
                 column_aliases: list[str] = [],
                 #having=empty_where_function,
                 extrapolation=False, extrapolation_method='linear', extrapolation_cache_size=100):
        self._name = name


        self._aggregate_initializers = aggregate_initializers
        # TODO extrapolation arguments
        self._extrapolation = extrapolation
        self._extrapolation_method = extrapolation_method
        self._extrapolation_cache_size = extrapolation_cache_size
        self._groupby_rows = defaultdict(lambda: Group(aggregate_initializers)) if not self._extrapolation else \
            defaultdict(lambda: GroupExtrapolation(aggregate_initializers, extrapolation_method=self._extrapolation_method, cache_size=self._extrapolation_cache_size))
        self._groupby_columns = groupby_columns
        self._where = where
        self._column_aliases = self._determine_column_aliases(column_aliases)
        #self._having = having  # TODO

    # row is dict for now
    def insert(self, row):
        if not self._where(**row):
            return False

        groupby_values = tuple(row[key] for key in self._groupby_columns)
        self._groupby_rows[groupby_values].insert(row)
        #return True  # TODO

    def delete(self, row):
        groupby_values = tuple(row[key] for key in self._groupby_columns)
        self._groupby_rows[groupby_values].delete(row)
        #return True  # TODO


    def update(self, old_row, new_row):
        self.delete(old_row)
        self.insert(new_row)


    @property
    def _base_column_names(self):
        return self._groupby_columns + list(map(lambda x: str(x), self._aggregate_initializers))


    def get_rows(self):
        for key, value in self._groupby_rows.items():
            yield list(key) + value.get_result()


    def extrapolate(self, extrapolation_timestamp=None):
        if extrapolation_timestamp is None:
            extrapolation_timestamp = time.time()
        if not self._extrapolation:
            raise ValueError('Attempted to extrapolate a groupby in which extrapolation is not enabled')
        for key, value in self._groupby_rows.items():
            yield list(key) + value.extrapolate(extrapolation_timestamp)

    def to_string_extrapolated(self, extrapolation_timestamp=None):
        return tabulate(self.extrapolate(extrapolation_timestamp), headers=self.column_names, tablefmt=constants.TABULATE_FORMAT)


    @property
    def name(self):
        return self._name

    @property
    def columns_count(self):
        return len(self._groupby_columns) + len(self._aggregate_initializers)

    @property
    def column_names(self):
        return self._column_aliases


    def _determine_column_aliases(self, column_aliases: list[str]):
        # Padding column_aliases list to columns count
        column_names = column_aliases + [''] * (self.columns_count - len(column_aliases))
        for i in range(len(column_names)):
            if column_names[i] == '':
                column_names[i] = self._base_column_names[i]
        return column_names


    def _select(self, input_rows, column_names=None, where=empty_where_function):
        #if column_names is None:
        #    column_names = self.column_names
        where_rows = filter(lambda x: where(*x), input_rows)
        if column_names is None:
            return where_rows
        column_indexes = utilities.list.find_multiple(self.column_names, column_names)
        rows = map(lambda x: utilities.list.index_many(x, column_indexes), where_rows)
        return rows


    def select(self, column_names=None, where=empty_where_function):
        return self._select(self.get_rows(), column_names, where)

    def select_extrapolated(self, column_names=None, where=empty_where_function, extrapolation_timestamp=None):
        rows = self.extrapolate(extrapolation_timestamp=extrapolation_timestamp)
        return self._select(rows, column_names, where)


    def __str__(self):
        return tabulate(self.get_rows(), headers=self.column_names, tablefmt=constants.TABULATE_FORMAT)


    # To store groupbys in sets
    def __eq__(self, other):
        return self._name == other.name


    # To store groupbys in sets
    def __hash__(self):
        return hash(self._name)
