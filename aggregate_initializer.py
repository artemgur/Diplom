from dataclasses import dataclass
from typing import Callable

from aggregate_functions import Aggregate


@dataclass(frozen=True)
class AggregateInstance:
    column_name: str
    aggregate: Aggregate


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


    def init_aggregate(self, column_cache: Callable) -> AggregateInstance:
        return AggregateInstance(column_name=self._column_name, aggregate=self._aggregate_type(**self._init_parameters, column_cache=column_cache))


    @property
    def column_to_cache(self) -> str | None:
        # TODO combine cache
        if self._aggregate_type.needs_column_cache():
            return self._column_name
        return None




