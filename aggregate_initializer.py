from dataclasses import dataclass

from aggregate_functions import Aggregate


class AggregateInitializer:
    def __init__(self, column_name: str, aggregate_type: type[Aggregate], init_parameters: dict=None):
        if init_parameters is None:
            init_parameters = {}
        self._column_name = column_name
        self._aggregate_type = aggregate_type
        self._init_parameters = init_parameters


    # TODO short and long versions without and with init_parameters?
    def __str__(self):
        return self._aggregate_type.function_name() + '(' + self._column_name + ')'


    def init_aggregate(self):
        return AggregateTuple(column_name=self._column_name, aggregate=self._aggregate_type(**self._init_parameters))


@dataclass(frozen=True)
class AggregateTuple:
    column_name: str
    aggregate: Aggregate

