from collections import defaultdict

from tabulate import tabulate

from aggregate_initializer import AggregateInitializer
from group import Group
import constants.output
from utilities.empty_functions import empty_where_function


# TODO optional ordered storage?
# It should be easy to make ordered storage based on groupby column, but harder – based on aggregate
class Groupby:
    # groupby_columns – list[str] (or maybe tuple[str]) of column names
    # _groupby_rows – dict. Key – tuple of values of groupby columns, values – list of aggregate functions
    def __init__(self, groupby_columns: list[str], agg_list_initializer: list[AggregateInitializer], where=empty_where_function, having=empty_where_function):
        self._agg_list_initializer = agg_list_initializer
        self._groupby_rows = defaultdict(lambda: Group(agg_list_initializer))
        self._groupby_columns = groupby_columns
        self._where = where
        self._having = having  # TODO

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
    def column_names(self):
        return self._groupby_columns + list(map(lambda x: str(x), self._agg_list_initializer))


    def get_rows(self):
        for key, value in self._groupby_rows.items():
            yield list(key) + value.get_result()


    def __str__(self):
        return tabulate(self.get_rows(), headers=self.column_names, tablefmt=constants.output.TABULATE_FORMAT)
