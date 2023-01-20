import json
import time

from api import json_api
from constants import SLEEP_TIME_BETWEEN_QUERIES
from groupby import Groupby
from sources import Source


def run(source: Source, queries_dict: dict, responses_dict: dict, view_names: dict):
    while True:
        source.listen()
        source_name = 'source.' + source.name
        #print(source_name)
        #print(queries_dict)
        if source_name in queries_dict:
            #print(1)
            request_dict = json.loads(queries_dict[source_name])
            run_source_queries(source, request_dict, responses_dict, view_names)
            del queries_dict[source_name]
        for view in source.views:
            view_name = 'view.' + view.name
            if view_name in queries_dict:
                request_dict = json.loads(queries_dict[view_name])
                run_queries(source, view, request_dict, responses_dict, view_names)
                del queries_dict[view_name]
        time.sleep(SLEEP_TIME_BETWEEN_QUERIES)


def run_source_queries(source: Source, request_dict: dict, responses_dict: dict, view_names: dict):
    match json_api.query_type(request_dict):
        case 'CREATE VIEW':
            create_view(source, request_dict, responses_dict, view_names)


def run_queries(source: Source, view: Groupby, request_dict: dict, responses_dict: dict, view_names: dict):
    match json_api.query_type(request_dict):
        case 'SELECT':
            select(view, request_dict, responses_dict)
        case 'SELECT EXTRAPOLATED':
            select_extrapolated(view, request_dict, responses_dict)
        case 'DROP VIEW':
            drop_view(source, view, request_dict, responses_dict, view_names)


@json_api.error_decorator
def create_view(source: Source, request_dict: dict, responses_dict: dict, view_names: dict):
    name = json_api.name(request_dict)
    if name in view_names:
        raise ValueError(f'View with name {name} already exists')
    groupby_columns = json_api.groupby_columns(request_dict)
    where = json_api.where(request_dict)
    parameters = json_api.parameters(request_dict)
    column_aliases = json_api.column_aliases(request_dict)
    aggregate_initializers = json_api.aggregate_initializers(request_dict)
    view = Groupby(name=name, groupby_columns=groupby_columns, aggregate_initializers=aggregate_initializers,
                   where=where, column_aliases=column_aliases, **parameters)
    view_names[name] = 1
    #source_name = json_api.view_source_name(request_dict)
    #source = sources.base.sources[source_name]
    source.subscribe(view)
    json_api.send_response('OK', request_dict, responses_dict)

@json_api.error_decorator
def drop_view(source: Source, view: Groupby, request_dict: dict, responses_dict: dict, view_names: dict):
    name = json_api.name(request_dict)
    source.unsubscribe(view)
    del view_names[name]
    json_api.send_response('OK', request_dict, responses_dict)


@json_api.error_decorator
def select(view: Groupby, request_dict: dict, responses_dict: dict):
    #where = json_api.where(request_dict)
    # TODO where having, order by
    # TODO better result format
    result = str(view)
    json_api.send_response(result, request_dict, responses_dict)

@json_api.error_decorator
def select_extrapolated(view: Groupby, request_dict: dict, responses_dict: dict):
    #where = json_api.where(request_dict)
    # TODO where having, order by
    # TODO better result format
    result = view.to_string_extrapolated()
    json_api.send_response(result, request_dict, responses_dict)