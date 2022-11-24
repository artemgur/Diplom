from collections import deque


class TableCache:
    def __init__(self, column_names, where_condition):
        self._rows = deque()  # Probably will be better than list
        self._column_names = column_names
        self._where_condition = where_condition


    def insert(self, row: dict):
        if not self._where_condition(**row):
            return False
        new_row = []
        for column in self._column_names:
            # TODO check insert conditions
            new_row.append(row[column])
        self._rows.append(new_row)
        return True

    def get_column(self, column_name):
        column_id = self._column_names.index(column_name)
        return map(lambda x: x[column_id], self._rows)  # TODO convert to list?

