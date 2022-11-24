class OrderBy:
    # columns_to_sort – list of column names. TODO make sort by expressions possible?
    # sort_modes - list of asc/desc
    def __init__(self, columns_to_sort, sort_modes):
        self._columns_to_sort = columns_to_sort
        self._sort_modes = sort_modes