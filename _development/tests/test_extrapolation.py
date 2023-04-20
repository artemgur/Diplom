import time

from aggregate_functions import Sum, Max
from aggregate_initializer import AggregateInitializer
from materialized_view import MaterializedView


base_time = 0.1


groupby = MaterializedView('view1', groupby_columns=['a'], aggregate_initializers=[AggregateInitializer('b', Sum), AggregateInitializer('c', Max)],
                           extrapolation=True, extrapolation_method='quintic')
groupby.insert({'a': 1, 'b': 1, 'c': 0})
groupby.insert({'a': 2, 'b': -1, 'c': 0})
for i in range(10):
    time.sleep(base_time)
    groupby.insert({'a': 1, 'b': 1, 'c': (i + 1) ** 2})
    groupby.insert({'a': 2, 'b': -2, 'c': (i + 1) ** 3})
time.sleep(base_time * 2)
print(groupby)
print(groupby.to_string_extrapolated())
