from aggregate_functions import Sum, Avg
from aggregate_initializer import AggregateInitializer
from groupby import Groupby
from sources.debezium import DebeziumSource
from table_cache_base import TableCacheBase


source = DebeziumSource('debezium_source', 'postgres_source.public.my_table',
                        auto_offset_reset='earliest', group_id=None, bootstrap_servers=['kafka:9092'], consumer_timeout_ms=1000)

groupby = Groupby(groupby_columns=['b'], agg_list_initializer=[AggregateInitializer('a', Sum), AggregateInitializer('c', Avg)])
table_cache = TableCacheBase(groupby=groupby)

source.subscribe(table_cache)

source.listen()

print(str(groupby))
