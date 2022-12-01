from aggregate_initializer import AggregateInitializer, AggregateTuple


class AggregateList:
    def __init__(self, agg_list_initializer: list[AggregateInitializer]):
        self._aggregate_list: list[AggregateTuple] = [aggregate_initializer.init_aggregate() for aggregate_initializer in agg_list_initializer]


    def insert(self, row):
        for i, aggregate_tuple in enumerate(self._aggregate_list):
            self._aggregate_list[i].aggregate.add_value(row[aggregate_tuple.column_name])

    def delete(self, row):
        for i, aggregate_tuple in enumerate(self._aggregate_list):
            self._aggregate_list[i].aggregate.remove_value(row[aggregate_tuple.column_name])

    def get_result(self):
        return list(map(lambda aggregate_tuple: aggregate_tuple.aggregate.get_result(), self._aggregate_list))
