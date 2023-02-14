class OrderByMode:
    def __init__(self, desc=False, nulls_first=False):
        self._desc = desc
        self._nulls_first = nulls_first


    @property
    def desc(self):
        return self._desc


    @property
    def nulls_first(self):
        return self._nulls_first