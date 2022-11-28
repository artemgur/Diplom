from collections import defaultdict

from aggregate_list import AggregateList


class Groupby:
    # groupby_columns – list[str] (or maybe tuple[str]) of column names
    # _groupby_rows – dict. Key – tuple of values of groupby columns, values – list of aggregate functions
    def __init__(self, groupby_columns, agg_list_initializer, column_names):
        self._column_names = column_names  # TODO
        self._agg_list_initializer = agg_list_initializer # TODO one column can have multiple aggregates
        self._groupby_rows = defaultdict(lambda: AggregateList(agg_list_initializer))
        self._groupby_columns = groupby_columns

    # row is dict for now
    def insert(self, row):
        groupby_values = tuple(row[key] for key in self._groupby_columns)
        self._groupby_rows[groupby_values].add_row(row)
        #return True  # TODO

    def delete(self, row):
        groupby_values = tuple(row[key] for key in self._groupby_columns)
        self._groupby_rows[groupby_values].remove_row(row)
        #return True  # TODO


    def get_result(self):
        for key, value in self._groupby_rows.items():
            yield list(key) + value.get_result()
