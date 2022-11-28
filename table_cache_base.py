from groupby import Groupby
from utilities.empty_functions import empty_where_function


# TODO type conversion
# Bridge pattern
class TableCacheBase:
    def __init__(self, groupby: Groupby, where_condition=empty_where_function):
        self._groupby = groupby
        self._where_condition = where_condition


    def insert(self, row: dict):
        if not self._where_condition(**row):
            return False
        if self._groupby is not None:
            self._groupby.insert(row)
        return True

    def delete(self, row: dict):
        if self._groupby is not None:
            self._groupby.delete(row)
        return True

    def update(self, old_row: dict, new_row: dict):
        self.delete(old_row)
        # The case when old row doesn't exist is valid, old row can be not in table cache because it is invalid for WHERE
        #if not delete_result:
        #    raise ValueError("Attempted to update row which doesn't exist")  # TODO do something better than exception
        self.insert(new_row)
