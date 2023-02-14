import time

import requests

import constants
from _development.tests.utilities import print_response


create_source = {'query_type': 'CREATE SOURCE',
                 'name': 'test_source', 'type': 'SimpleTestSource',
                 'parameters': {'columns': ['a', 'b', 'c'], 'rows_to_add': 3}}
create_view = {'query_type': 'CREATE VIEW',
               'name': 'test_view', 'view_source_name': 'test_source', 'groupby_columns': ['a'],
               'parameters': {'extrapolation': True},
               'aggregates': [{'function': 'Sum', 'column': 'b'}, {'function': 'Avg', 'column': 'c'}]}
select = {'query_type': 'SELECT', 'name': 'test_view', 'where': 'a < 3 or "sum(b)" % 2 == 1', 'orderby': [['a', 'DESC'], 'sum(b)']}
print_response(requests.post(f'http://localhost:{constants.SERVER_PORT}', json=create_source))
#print('Source created')
print_response(requests.post(f'http://localhost:{constants.SERVER_PORT}', json=create_view))
#print('View created')
time.sleep(1)
select_response = requests.post(f'http://localhost:{constants.SERVER_PORT}', json=select)
print(select_response.content.decode())

