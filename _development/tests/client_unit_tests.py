import time
import unittest

import requests

import constants

from subprocess_utilities import start_subprocess, terminate_shell_subprocess


create_source = {'query_type': 'CREATE SOURCE',
                 'name': 'test_source', 'type': 'SimpleTestSource',
                 'parameters': {'columns': ['a', 'b', 'c'], 'rows_to_add': 3}}
create_view = {'query_type': 'CREATE MATERIALIZED VIEW',
               'name': 'test_view', 'view_source_name': 'test_source', 'groupby_columns': ['a'],
               'parameters': {'extrapolation': True},
               'aggregates': [{'function': 'Sum', 'column': 'b'}, {'function': 'Avg', 'column': 'c'}]}
select = {'query_type': 'SELECT', 'name': 'test_view', 'where': 'a < 3 or "sum(b)" % 2 == 1', 'orderby': [['a', 'DESC'], 'sum(b)'], 'format': 'tabulate'}
drop_view = {'query_type': 'DROP MATERIALIZED VIEW', 'name': 'test_view'}
drop_source = {'query_type': 'DROP SOURCE', 'name': 'test_source'}


def start_main_decorator(func):
    def wrapper(*args, **kwargs):
        process = start_subprocess('python ../../main.py')
        time.sleep(5)
        try:
            func(*args, **kwargs)
        finally:
            terminate_shell_subprocess(process)
    return wrapper

class ClientUnitTests(unittest.TestCase):
    def assert_response(self, response: requests.Response, status_code=200, error_message=''):
        self.assertEqual(response.status_code, status_code)
        if error_message:
            self.assertEqual(response.content.decode(), error_message)

    def create_source(self):
        self.assert_response(requests.post(f'http://localhost:{constants.SERVER_PORT}', json=create_source))

    def create_view(self):
        self.assert_response(requests.post(f'http://localhost:{constants.SERVER_PORT}', json=create_view))

    def select(self):
        select_response = requests.post(f'http://localhost:{constants.SERVER_PORT}', json=select)
        print(select_response.content.decode())

    def drop_view(self):
        self.assert_response(requests.post(f'http://localhost:{constants.SERVER_PORT}', json=drop_view))

    def drop_source(self):
        self.assert_response(requests.post(f'http://localhost:{constants.SERVER_PORT}', json=drop_source))

    @start_main_decorator
    def test_select(self):
        self.create_source()
        self.create_view()
        self.select()

    @start_main_decorator
    def test_drop_view(self):
        self.create_source()
        self.create_view()
        self.drop_view()
        self.create_view()
        self.select()

    @start_main_decorator
    def test_drop_source(self):
        self.create_source()
        self.create_view()
        self.drop_source()
        self.create_source()
        self.create_view()
        self.select()