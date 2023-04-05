import traceback

import utilities.reflection
from aggregate_initializer import AggregateInitializer
from aggregate_functions import *
import constants
from api import select_where_parser, view_where_parser
from orderby import OrderBy
from utilities.empty_functions import empty_where_function


def get_target(json_dict: dict) -> str:
    if query_type(json_dict) in ['CREATE SOURCE']:
        return constants.MAIN_PROCESS_NAME
    if query_type(json_dict) in ['CREATE MATERIALIZED VIEW']:
        return 'source.' + view_source_name(json_dict)
    if query_type(json_dict) in ['DROP SOURCE']:
        return 'source.' + name(json_dict)
    if query_type(json_dict) in ['SELECT', 'SELECT EXTRAPOLATED', 'DROP MATERIALIZED VIEW']:
        return 'view.' + name(json_dict)
    # TODO

def query_type(json_dict: dict) -> str:
    return json_dict['query_type'].upper()


def name(json_dict: dict) -> str:
    return json_dict['name']


def view_source_name(json_dict: dict) -> str:
    return json_dict['view_source_name']


def type(json_dict: dict) -> str:
    return json_dict['type']


def parameters(json_dict: dict) -> dict:
    return json_dict['parameters'] if 'parameters' in json_dict else {}


def groupby_columns(json_dict: dict) -> list:
    return json_dict['groupby_columns']


def column_aliases(json_dict: dict) -> list:
    return json_dict['column_aliases'] if 'column_aliases' in json_dict else []

def columns(json_dict: dict):
    return json_dict['columns'] if 'columns' in json_dict else None



def aggregate_initializers(json_dict: dict) -> list[AggregateInitializer]:
    result = []
    for agg_initializer_dict in json_dict['aggregates']:
        function_name = agg_initializer_dict['function']
        column_name = agg_initializer_dict['column']
        parameters = agg_initializer_dict['parameters'] if 'parameters' in agg_initializer_dict else {}
        function_type = utilities.reflection.str_to_type(function_name)
        if not issubclass(function_type, Aggregate):
            raise ValueError(f'String "{function_type}" is not an Aggregate')
        agg_initializer = AggregateInitializer(column_name, function_type, parameters)
        result.append(agg_initializer)
    return result


def send_response(response, request_dict: dict, response_dict: dict, success=True):
    success_str = '1' if success else '0'
    request_uuid = request_dict['request_uuid']
    response_dict[request_uuid] = success_str + response


def select_where(json_dict: dict, columns: list[str]):
    if 'where' not in json_dict:
        return empty_where_function
    return select_where_parser.parse(json_dict['where'], columns)


def view_where(json_dict: dict):
    if 'where' not in json_dict:
        return empty_where_function
    return view_where_parser.parse(json_dict['where'])


def extrapolation_timestamp(json_dict: dict):
    return json_dict['extrapolation_timestamp'] if 'extrapolation_timestamp' in json_dict else None


def orderby(json_dict: dict):
    if 'orderby' not in json_dict:
        return []
    result = []
    for orderby_ in json_dict['orderby']:
        if isinstance(orderby_, list):
            column_name = orderby_[0]
            desc = orderby_[1].upper() == 'DESC'
            result.append(OrderBy(column_name=column_name, desc=desc))
        else:
            result.append(OrderBy(column_name=orderby_, desc=False))
    return result


def format(json_dict: dict):
    return json_dict['format'] if 'format' in json_dict else 'csv'



def error_decorator(func):
    def wrapper(process, request_dict, *args, **kwargs):
        responses_dict = process.responses_dict

        try:
            func(process, request_dict, *args, **kwargs)
        except Exception as e:
            send_response(str(e), request_dict, responses_dict, success=False)
            traceback.print_exc()
    return wrapper


