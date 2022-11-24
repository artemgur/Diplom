from collections import deque
from datetime import datetime

from table_cache import TableCache


class TableCacheTTL(TableCache):
    def __init__(self, column_names, ttl, where_condition):
        super().__init__(column_names, where_condition)
        self._row_timestamp = deque()
        self._ttl = ttl

    # TODO condition for insert?
    # row is dict for now
    def insert(self, row: dict):
        is_insert_successful = super().insert(row)
        if is_insert_successful:
            self._row_timestamp.append(datetime.now())

    def remove_old_rows(self):
        # First rows are supposed to be older
        while self._row_timestamp[0] + self._ttl >= datetime.now(): # TODO check if it works
            # TODO update groupbys
            self._row_timestamp.popleft()
            self._rows.popleft()
