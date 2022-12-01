from collections import defaultdict

from tabulate import tabulate

from aggregate_initializer import AggregateInitializer
from aggregate_list import AggregateList
import constants.output


class Groupby:
    # groupby_columns – list[str] (or maybe tuple[str]) of column names
    # _groupby_rows – dict. Key – tuple of values of groupby columns, values – list of aggregate functions
    def __init__(self, groupby_columns: list[str], agg_list_initializer: list[AggregateInitializer]):
        self._agg_list_initializer = agg_list_initializer
        self._groupby_rows = defaultdict(lambda: AggregateList(agg_list_initializer))
        self._groupby_columns = groupby_columns

    # row is dict for now
    def insert(self, row):
        groupby_values = tuple(row[key] for key in self._groupby_columns)
        self._groupby_rows[groupby_values].insert(row)
        #return True  # TODO

    def delete(self, row):
        groupby_values = tuple(row[key] for key in self._groupby_columns)
        self._groupby_rows[groupby_values].delete(row)
        #return True  # TODO


    @property
    def column_names(self):
        return self._groupby_columns + list(map(lambda x: str(x), self._agg_list_initializer))


    def get_rows(self):
        for key, value in self._groupby_rows.items():
            yield list(key) + value.get_result()


    def __str__(self):
        return tabulate(self.get_rows(), headers=self.column_names, tablefmt=constants.output.TABULATE_FORMAT)
