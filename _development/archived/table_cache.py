from collections import deque

from table_cache_base import TableCacheBase
from utilities.empty_functions import empty_where_function

# Logic partially moved to GroupCache

# Useful for groupbys which don't need cache if we want consistency for delete
# Update: it is not consistent for delete because of WHERE condition. Because we just don't store rows which don't match WHERE
class TableCache(TableCacheBase):
    def __init__(self, column_names, groupby, where_condition=empty_where_function):
        super().__init__(groupby, where_condition)
        self._rows = deque()  # Probably will be better than list
        self._column_names = column_names
        self._where_condition = where_condition


    def insert(self, row: dict):
        where_check_result = super().insert(row)  # TODO read more about super()
        if not where_check_result:
            return False  # TODO
        new_row = []
        for column in self._column_names:
            # TODO check insert conditions
            new_row.append(row[column])
        self._rows.append(new_row)
        return True


    def _are_rows_equal(self, row1, row2):
        for column in self._column_names:
            if row1[column] != row2[column]:
                return False
        return True


    def delete(self, row: dict):
        row_to_delete_id = -1
        for row_number, existing_row in enumerate(self._rows):
            if self._are_rows_equal(existing_row, row):
                row_to_delete_id = row_number
        if row_to_delete_id == -1:  # Row not found in cache
            return False
        del self._rows[row_to_delete_id]  # TODO check if it works
        super().delete(row)


    def get_column(self, column_name):
        column_id = self._column_names.index(column_name)
        return map(lambda x: x[column_id], self._rows)  # TODO convert to list?


    #def get_rows(self):
    #    groupby_rows




