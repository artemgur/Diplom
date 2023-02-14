from sources.debezium import DebeziumSource
from aggregate_functions import Sum, Avg, Max, Min
from aggregate_initializer import AggregateInitializer
from groupby import Groupby
from sources.base import sources

from multiprocess import Manager, Process
from time import sleep


def process1(namespace):
    source = DebeziumSource('debezium_source', 'postgres_source.public.my_table',
                            auto_offset_reset='earliest', group_id=None, bootstrap_servers=[
            'kafka:9092'], consumer_timeout_ms=1000)

    #global sources

    groupby = Groupby(groupby_columns=['b'], aggregate_initializers=[AggregateInitializer('a', Sum),
                                                                     AggregateInitializer('a', Max),
                                                                     AggregateInitializer('a', Min),
                                                                     AggregateInitializer('c', Avg)])

    source.subscribe(groupby)

    source.listen()
    namespace.sources = sources
    print(namespace.sources)
    #print(namespace.sources['debezium_source']._subscribed_materialized_views)
    #print(namespace.sources['debezium_source']._subscribed_materialized_views[0])


def process2(namespace):
    print(namespace.sources)
    print(namespace.sources['debezium_source']._subscribed_materialized_views[0])


if __name__ == '__main__':



    mgr = Manager()
    ns = mgr.Namespace()
    ns.sources = sources




    p1 = Process(target=process1, args=(ns,))
    p2 = Process(target=process2, args=(ns,))
    p1.start()
    sleep(5)
    p2.start()
    while True:
        ...