from utilities.list import are_lists_equal


class TableData:
    def __init__(self, column_names: list[str]):
        self._column_indexes = {column_names[i]: i for i in range(len(column_names))}
        self._rows = []

    def insert(self, row: dict):
        if len(self._column_indexes) == 0:
            return

        new_row_list = self._row_dict_to_list(row)

        self._rows.append(new_row_list)
        # Insert is always successful, no need for return value?

    def delete(self, row: dict) -> bool:
        if len(self._column_indexes) == 0:
            return True

        row_as_list = self._row_dict_to_list(row)

        row_to_delete_id = -1
        for row_number, existing_row in enumerate(self._rows):
            if are_lists_equal(existing_row, row_as_list):
                row_to_delete_id = row_number
        if row_to_delete_id == -1:  # Row not found in cache
            return False
        del self._rows[row_to_delete_id]  # TODO check if it works
        return True

    #def update(self, old_row: dict, new_row: dict):
    #    delete_result = self.delete(old_row)
    #    self.insert(new_row)
    #    return delete_result

    def _row_dict_to_list(self, row):
        row_list = []
        for column in self._column_indexes:
            row_list.append(row[column])
        return row_list




    def get_column(self, column_name: str):
        column_index = self._column_indexes[column_name]
        for row in self._rows:
            yield row[column_index]

    def get_column_lambda(self, column_name: str):
        if column_name is None:
            return None
        return lambda: self.get_column(column_name)
