import json
import time
from multiprocessing import Manager, Process

from . import server
from . import source_process
import constants
from api import json_api
import utilities.reflection
import utilities.list
from materialized_view import MaterializedView
# TODO improve import
import sources
from sources import *


def start_main_process():
    main_process = MainProcess()
    main_process.run()


class MainProcess:
    def __init__(self):
        self.sources_to_process: dict[str, Process] = {}

        manager = Manager()

        self.queries_dict = manager.dict()
        self.responses_dict = manager.dict()
        self.view_names = manager.dict()


    def run(self):
        self.start_server()

        while True:
            self.check_queries()

            time.sleep(constants.SLEEP_TIME_BETWEEN_QUERIES)


    def start_server(self):
        """
        Starts server process.
        """
        server_process = Process(target=server.start_handler, args=(self.queries_dict, self.responses_dict, self.view_names))
        server_process.start()


    def check_queries(self):
        """
        Checks if there are queries to the main process. If such queries are found, executes them.
        """
        if constants.MAIN_PROCESS_NAME in self.queries_dict:
            queries = self.queries_dict[constants.MAIN_PROCESS_NAME]
            for value in queries:
                request_dict = json.loads(value)
                self.run_queries(request_dict)
            self.queries_dict[constants.MAIN_PROCESS_NAME] = utilities.list.difference(self.queries_dict[constants.MAIN_PROCESS_NAME], queries)


    def run_queries(self, request_dict: dict):
        """
        Maps queries to relevant functions.
        """
        match json_api.query_type(request_dict):
            case 'CREATE SOURCE':
                self.create_source(request_dict)


    @json_api.error_decorator
    def create_source(self, request_dict: dict):
        """
        Executes CREATE SOURCE query.
        """
        source_type_str = json_api.type(request_dict)
        name = json_api.name(request_dict)
        if name in self.sources_to_process:
            if self.sources_to_process[name].is_alive():
                raise ValueError(f'Source {name} already exists')
            else:
                del self.sources_to_process[name]
        parameters = json_api.parameters(request_dict)
        source_type = utilities.reflection.str_to_type(source_type_str)
        if not issubclass(source_type, Source):
            raise ValueError(f'{source_type} is not a valid source type')
        source = source_type(name=name, **parameters)
        self.start_source_process(source)
        json_api.send_response('OK', request_dict, self.responses_dict)


    def start_source_process(self, source: Source):
        source_proc = Process(target=source_process.start_source_process, args=(source, self.queries_dict, self.responses_dict, self.view_names))
        self.sources_to_process[source.name] = source_proc
        source_proc.start()