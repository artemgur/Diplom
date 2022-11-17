from collections import deque
from datetime import datetime


class TableCache:
    def __init__(self, column_names, ttl, insert_conditions):
        self._rows = deque()  # Probably will be better than list
        self._column_names = column_names
        # TODO not all table caches will have TTL. Move TTL to subclass?
        self._row_timestamp = deque()
        self._ttl = ttl
        self._insert_conditions = insert_conditions

    # TODO condition for insert?
    # row is dict for now
    def insert(self, row: dict):
        new_row = []
        for column in self._column_names:
            # TODO check insert conditions
            new_row.append(row[column])
        self._rows.append(new_row)
        self._row_timestamp.append(datetime.now())

    def get_column(self, column_name):
        column_id = self._column_names.index(column_name)
        return map(lambda x: x[column_id], self._rows)  # TODO convert to list?

    def remove_old_rows(self):
        # First rows are supposed to be older
        while self._row_timestamp[0] + self._ttl >= datetime.now(): # TODO check if it works
            # TODO update groupbys
            self._row_timestamp.popleft()
            self._rows.popleft()
