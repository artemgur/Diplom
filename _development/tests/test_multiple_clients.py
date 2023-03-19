import time
from multiprocessing import Manager, Process

import requests

import constants
from _development.tests.test_utilities import print_response


# Uncomment print statements in server file to check that requests are actually handled in multiple threads
def run():
    process1 = Process(target=run_process, args=(1,))
    process1.start()

    process2 = Process(target=run_process, args=(2,))
    process2.start()


def run_process(process_id):
    process_prefix = f'Process {process_id}'
    create_source = {'query_type': 'CREATE SOURCE',
                     'name': f'test_source{process_id}', 'type': 'SimpleTestSource',
                     'parameters': {'columns': ['a', 'b', 'c'], 'rows_to_add': 3}}
    create_view = {'query_type': 'CREATE MATERIALIZED VIEW',
                   'name': f'test_view{process_id}', 'view_source_name': f'test_source{process_id}', 'groupby_columns': ['a'],
                   'parameters': {'extrapolation': True},
                   'aggregates': [{'function': 'Sum', 'column': 'b'}, {'function': 'Avg', 'column': 'c'}]}
    select = {'query_type': 'SELECT', 'name': f'test_view{process_id}', 'where': 'a < 3 or "sum(b)" % 2 == 1'}
    print_response(requests.post(f'http://localhost:{constants.SERVER_PORT}', json=create_source), prefix=process_prefix)
    # print('Source created')
    print_response(requests.post(f'http://localhost:{constants.SERVER_PORT}', json=create_view), prefix=process_prefix)
    # print('View created')
    time.sleep(1)
    select_response = requests.post(f'http://localhost:{constants.SERVER_PORT}', json=select)
    print(select_response.content.decode(), process_prefix)
    print(process_prefix, 'finished')


if __name__ == '__main__':
    run()