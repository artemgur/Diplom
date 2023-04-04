import json
import time

from api import json_api
from constants import SLEEP_TIME_BETWEEN_QUERIES
from groupby import Groupby
from sources import Source
import rows_formatter
import utilities.list


views_to_drop = {}


def run(source: Source, queries_dict: dict, responses_dict: dict, view_names: dict):
    global views_to_drop
    drop_source = False
    while True:
        source.listen()
        source_name = 'source.' + source.name
        if source_name in queries_dict:
            queries = queries_dict[source_name]
            for value in queries:
                #queries_dict[source_name].remove(value)
                request_dict = json.loads(value)
                run_source_queries(source, request_dict, responses_dict, view_names)
            queries_dict[source_name] = utilities.list.difference(queries_dict[source_name], queries)
            #del queries_dict[source_name]
        for view in source.views:
            view_name = 'view.' + view.name
            if view_name in queries_dict:
                queries = queries_dict[view_name]
                for value in queries:
                    #queries_dict[view_name].remove(value)
                    request_dict = json.loads(value)
                    if json_api.query_type(request_dict) == 'DROP SOURCE':
                        drop_source = True
                        finalize_process(source, view_names)
                        json_api.send_response('OK', request_dict, responses_dict)
                    else:
                        run_queries(source, view, request_dict, responses_dict, view_names)
                queries_dict[view_name] = utilities.list.difference(queries_dict[view_name], queries)
                #del queries_dict[view_name]
        for view_name in views_to_drop:
            request_dict, view = views_to_drop[view_name]
            drop_view_actual(request_dict, responses_dict, source, view, view_names)
        views_to_drop = {}

        if drop_source:
            break

        time.sleep(SLEEP_TIME_BETWEEN_QUERIES)


def run_source_queries(source: Source, request_dict: dict, responses_dict: dict, view_names: dict):
    match json_api.query_type(request_dict):
        case 'CREATE MATERIALIZED VIEW':
            create_view(request_dict, responses_dict, source, view_names)


def run_queries(source: Source, view: Groupby, request_dict: dict, responses_dict: dict, view_names: dict):
    match json_api.query_type(request_dict):
        case 'SELECT' | 'SELECT EXTRAPOLATED':
            select(request_dict, responses_dict, view)
        case 'DROP MATERIALIZED VIEW':
            drop_view(request_dict, responses_dict, source, view, view_names)


@json_api.error_decorator
def create_view(request_dict: dict, responses_dict: dict, source: Source, view_names: dict):
    name = json_api.name(request_dict)
    if name in view_names:
        raise ValueError(f'View with name {name} already exists')
    groupby_columns = json_api.groupby_columns(request_dict)
    where = json_api.view_where(request_dict)
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


def drop_view(request_dict: dict, responses_dict: dict, source: Source, view: Groupby, view_names: dict):
    views_to_drop[view.name] = (request_dict, view)


@json_api.error_decorator
def drop_view_actual(request_dict: dict, responses_dict: dict, source: Source, view: Groupby, view_names: dict):
    name = json_api.name(request_dict)
    source.unsubscribe(view)
    del view_names[name]
    json_api.send_response('OK', request_dict, responses_dict)


@json_api.error_decorator
def select(request_dict: dict, responses_dict: dict, view: Groupby):
    columns = json_api.columns(request_dict)
    where = json_api.select_where(request_dict, view.column_names)
    orderby_list = json_api.orderby(request_dict)

    if json_api.query_type(request_dict) == 'SELECT EXTRAPOLATED':
        extrapolation_timestamp = json_api.extrapolation_timestamp(request_dict)
        rows = view.select_extrapolated(column_names=columns, where=where, extrapolation_timestamp=extrapolation_timestamp)
    else:
        rows = view.select(column_names=columns, where=where)

    if len(orderby_list) > 0:
        rows = view.orderby(column_names=columns, rows=rows, orderby_list=orderby_list)
    if columns is None:
        columns = view.column_names
    result = rows_formatter.format(rows, column_names=columns, format_type=json_api.format(request_dict))
    json_api.send_response(result, request_dict, responses_dict)



def finalize_process(source: Source, view_names: dict):
    for view in source.views:
        del view_names[view.name]
