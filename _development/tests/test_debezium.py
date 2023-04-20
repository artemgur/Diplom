from aggregate_functions import Sum, Avg, Max, Min
from aggregate_initializer import AggregateInitializer
from materialized_view import MaterializedView
from sources.debezium import DebeziumSource


source = DebeziumSource('debezium_source', 'postgres_source.public.demo_table',
                        auto_offset_reset='earliest', group_id=None, bootstrap_servers=['kafka:9092'], consumer_timeout_ms=1000)

groupby = MaterializedView('view1', groupby_columns=['b'], aggregate_initializers=[AggregateInitializer('a', Sum), AggregateInitializer('a', Max), AggregateInitializer('a', Min), AggregateInitializer('c', Avg)])

source.subscribe(groupby)

source.listen()

print(str(groupby))
