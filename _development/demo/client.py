import time
import atexit

import requests

import kafka_utilities
import constants
from _development.tests.test_utilities import print_response


topic_name = 'postgres_source.public.demo_table'


#atexit.register(lambda: kafka_utilities.delete_topic(topic_name))
create_source = {'query_type': 'CREATE SOURCE',
                 'name': 'demo_source', 'type': 'DebeziumSource',
                 'parameters': {'kafka_topic_name': topic_name, 'group_id': None, 'auto_offset_reset': 'earliest',
                                'bootstrap_servers': ['kafka:9092'], 'consumer_timeout_ms': 1000}}
create_view = {'query_type': 'CREATE MATERIALIZED VIEW',
               'name': 'demo_view', 'view_source_name': 'demo_source', 'groupby_columns': ['a'],
               'parameters': {'extrapolation': True},
               'aggregates': [{'function': 'Sum', 'column': 'b'}, {'function': 'Avg', 'column': 'c'}]}
select = {'query_type': 'SELECT', 'name': 'demo_view', 'orderby': [['a', 'DESC']], 'format': 'tabulate'}
print_response(requests.post(f'http://localhost:{constants.SERVER_PORT}', json=create_source))
print('Source created')
print_response(requests.post(f'http://localhost:{constants.SERVER_PORT}', json=create_view))
print('View created')
while True:
    time.sleep(2)
    print('before_select')
    select_response = requests.post(f'http://localhost:{constants.SERVER_PORT}', json=select)
    print('after_select')
    print(select_response.content.decode())

