import json
import time
# noinspection PyUnresolvedReferences
# TODO change to multiprocess for actual execution. multiprocessing is only for hints
from multiprocessing import Manager, Process

from . import server
from . import source_process
import constants
from api import json_api
import utilities.reflection
from groupby import Groupby
# TODO improve import
import sources
from sources import *


# TODO is it safe to make it global?
sources_to_process: dict[str, Process] = {}
#views_to_sources: dict[str, str] = {}


def run():
    manager = Manager()
    # dict:
    queries_dict = manager.dict()
    responses_dict = manager.dict()
    view_names = manager.dict()

    server_process = Process(target=server.start_handler, args=(queries_dict, responses_dict))
    server_process.start()

    while True:
        if constants.MAIN_PROCESS_NAME in queries_dict:
            request_dict = json.loads(queries_dict[constants.MAIN_PROCESS_NAME])
            run_queries(request_dict, responses_dict, queries_dict, view_names)
            del queries_dict[constants.MAIN_PROCESS_NAME]
        time.sleep(constants.SLEEP_TIME_BETWEEN_QUERIES)


# TODO response_dict for errors
def run_queries(request_dict: dict, responses_dict: dict, queries_dict: dict, view_names: dict):
    match json_api.query_type(request_dict):
        case 'CREATE SOURCE':
            create_source(request_dict, responses_dict, queries_dict, view_names)
        case 'DROP SOURCE':
            drop_source(request_dict, responses_dict)
    # TODO drop


@json_api.error_decorator
def create_source(request_dict: dict, responses_dict: dict, queries_dict: dict, view_names: dict):
    source_type_str = json_api.type(request_dict)
    name = json_api.name(request_dict)
    if name in sources_to_process:
        raise ValueError(f'Source {name} already exists')
    parameters = json_api.parameters(request_dict)
    # TODO
    source_type = utilities.reflection.str_to_type(source_type_str)
    if not issubclass(source_type, Source):
        raise ValueError(f'{source_type} is not a valid source type')
    source = source_type(name=name, **parameters)
    start_source_process(source, queries_dict, responses_dict, view_names)
    json_api.send_response('OK', request_dict, responses_dict)


@json_api.error_decorator
def drop_source(request_dict, responses_dict):
    name = json_api.name(request_dict)




def start_source_process(source: Source, request_dict: dict, responses_dict: dict, view_names: dict):
    source_proc = Process(target=source_process.run, args=(source, request_dict, responses_dict, view_names))
    sources_to_process[source.name] = source_proc
    source_proc.start()