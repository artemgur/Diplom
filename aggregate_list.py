from dataclasses import dataclass

from aggregate_functions import Aggregate


@dataclass
class AggregateTuple:
    column_name: str
    aggregate: Aggregate

    def __str__(self):
        return self.aggregate.function_name() + '(' + self.column_name + ')'


class AggregateList:
    # agg_list_initializer: dict, key – column name, value – lambda/function, which initializes aggregate
    def __init__(self, agg_list_initializer):
        self._aggregate_list: list[AggregateTuple] = [AggregateTuple(key, value()) for key, value in agg_list_initializer]
        #self._aggregate_list: Dict[str, Aggregate] = {key: value() for key, value in agg_list_initializer.items()}


    def add_row(self, row):
        for i, aggregate_tuple in enumerate(self._aggregate_list):
            self._aggregate_list[i].aggregate.add_value(row[aggregate_tuple.column_name])

    def remove_row(self, row):
        for i, aggregate_tuple in enumerate(self._aggregate_list):
            self._aggregate_list[i].aggregate.remove_value(row[aggregate_tuple.column_name])

    def get_result(self):
        return list(map(lambda aggregate_tuple: aggregate_tuple.aggregate.get_result(), self._aggregate_list))

    @property
    def column_names(self):
        return list(map(lambda x: str(x), self._aggregate_list))