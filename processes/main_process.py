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

    server_process = Process(target=server.start_handler, args=(queries_dict, responses_dict))
    server_process.start()

    while True:
        if constants.MAIN_PROCESS_NAME in queries_dict:
            request_dict = json.loads(queries_dict[constants.MAIN_PROCESS_NAME])
            run_queries(queries_dict, request_dict, responses_dict)
            del queries_dict[constants.MAIN_PROCESS_NAME]
        time.sleep(constants.SLEEP_TIME_BETWEEN_QUERIES)


# TODO response_dict for errors
def run_queries(queries_dict: dict, request_dict: dict, responses_dict: dict):
    match json_api.query_type(request_dict):
        case 'CREATE SOURCE':
            create_source(queries_dict, request_dict, responses_dict)
    # TODO drop


def create_source(queries_dict: dict, request_dict: dict, responses_dict: dict):
    source_type_str = json_api.type(request_dict)
    name = json_api.name(request_dict)
    parameters = json_api.parameters(request_dict)
    # TODO
    source_type = utilities.reflection.str_to_type(source_type_str)
    if not issubclass(source_type, Source):
        raise ValueError  # TODO do something better than exception
    #exec(f'result = {source_type}(name={name}, **{parameters})')
    source = source_type(name=name, **parameters)
    start_source_process(source, queries_dict, responses_dict)
    json_api.send_response('OK', request_dict, responses_dict)






def start_source_process(source: Source, request_dict: dict, responses_dict: dict):
    source_proc = Process(target=source_process.run, args=(source, request_dict, responses_dict))
    sources_to_process[source.name] = source_proc
    source_proc.start()