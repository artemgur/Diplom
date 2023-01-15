import json
import time

from api import json_api
from constants import SLEEP_TIME_BETWEEN_QUERIES
from groupby import Groupby
from sources import Source


# TODO View names can duplicate!
def run(source: Source, queries_dict: dict, responses_dict: dict):
    while True:
        source.listen()
        source_name = 'source.' + source.name
        #print(source_name)
        #print(queries_dict)
        if source_name in queries_dict:
            #print(1)
            request_dict = json.loads(queries_dict[source_name])
            run_source_queries(source, request_dict, responses_dict)
            del queries_dict[source_name]
        for view in source.views:
            view_name = 'view.' + view.name
            if view_name in queries_dict:
                request_dict = json.loads(queries_dict[view_name])
                run_queries(view, request_dict, responses_dict)
                del queries_dict[view_name]
        time.sleep(SLEEP_TIME_BETWEEN_QUERIES)


def run_source_queries(source: Source, request_dict: dict, responses_dict: dict):
    match json_api.query_type(request_dict):
        case 'CREATE VIEW':
            create_view(source, request_dict, responses_dict)
        case 'DROP VIEW':
            drop_view(source, request_dict, responses_dict)


def run_queries(view: Groupby, request_dict: dict, responses_dict: dict):
    match json_api.query_type(request_dict):
        case 'SELECT':
            select(view, request_dict, responses_dict)
        case 'SELECT EXTRAPOLATED':
            select_extrapolated(view, request_dict, responses_dict)



def create_view(source: Source, request_dict: dict, responses_dict: dict):
    name = json_api.name(request_dict)
    groupby_columns = json_api.groupby_columns(request_dict)
    where = json_api.where(request_dict)
    parameters = json_api.parameters(request_dict)
    aggregate_initializers = json_api.aggregate_initializers(request_dict)
    view = Groupby(name=name, groupby_columns=groupby_columns, aggregate_initializers=aggregate_initializers, where=where, **parameters)
    #source_name = json_api.view_source_name(request_dict)
    #source = sources.base.sources[source_name]
    source.subscribe(view)
    json_api.send_response('OK', request_dict, responses_dict)


def drop_view(source: Source, request_dict: dict, responses_dict: dict):
    name = json_api.name(request_dict)
    for view in source.views:
        if view.name


def select(view: Groupby, request_dict: dict, responses_dict: dict):
    #where = json_api.where(request_dict)
    # TODO where having, order by
    # TODO better result format
    result = str(view)
    json_api.send_response(result, request_dict, responses_dict)


def select_extrapolated(view: Groupby, request_dict: dict, responses_dict: dict):
    #where = json_api.where(request_dict)
    # TODO where having, order by
    # TODO better result format
    result = view.to_string_extrapolated()
    json_api.send_response(result, request_dict, responses_dict)