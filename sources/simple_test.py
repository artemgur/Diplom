import json
import random

from kafka import KafkaConsumer

from sources.base import Source


class SimpleTestSource(Source):
    def __init__(self, name, columns, rows_to_add=5, **kwargs):
        super().__init__(name)
        self._columns = columns
        self._rows_to_add = rows_to_add

    def listen(self):
        for i in range(self._rows_to_add):
            new_row = {}
            for column in self._columns:
                new_row[column] = random.randint(0, 5)
            self._update_materialized_views(None, new_row)