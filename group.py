from aggregate_initializer import AggregateInitializer, AggregateTuple
from table_data import TableData


# TODO rename. Maybe should be called GroupbyClause?
class Group:
    def __init__(self, agg_list_initializer: list[AggregateInitializer]):
        # Order shouldn't matter here
        columns_to_cache = list(set(filter(lambda x: x is not None, map(lambda x: x.column_to_cache, agg_list_initializer))))

        self._table_data = TableData(columns_to_cache)

        aggregate_cache_lambdas = map(lambda x: self._table_data.get_column_lambda(x.column_to_cache), agg_list_initializer)

        self._aggregate_list: list[AggregateTuple] = \
            [aggregate_initializer.init_aggregate(column_cache=aggregate_lambda) for aggregate_initializer, aggregate_lambda
                in zip(agg_list_initializer, aggregate_cache_lambdas)]
# TODO:
# Just init caches here for aggregates that need them. Then the aggregates just will use caches without changes to insert and delete. All changes will be in __init__
# Find a 2d list library for convenient and fast column slicing?
# Combine cache probably should be initialized in aggregate itself, in base __init__()?
# And cache probably should be used in base delete()?
# We also need list of column names which need caching to avoid storage of unneeded columns

    def insert(self, row):
        self._table_data.insert(row)
        for i, aggregate_tuple in enumerate(self._aggregate_list):
            self._aggregate_list[i].aggregate.insert(row[aggregate_tuple.column_name])

    def delete(self, row):
        delete_actually_removed_row = self._table_data.delete(row)
        if delete_actually_removed_row:
            for i, aggregate_tuple in enumerate(self._aggregate_list):
                self._aggregate_list[i].aggregate.delete(row[aggregate_tuple.column_name])
            return True
        return False

    #def update(self, old_row, new_row):
    #    self.delete(old_row)
    #    self.insert(new_row)

    def get_result(self):
        return list(map(lambda aggregate_tuple: aggregate_tuple.aggregate.get_result(), self._aggregate_list))
