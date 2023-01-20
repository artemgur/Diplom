import utilities.reflection
from aggregate_initializer import AggregateInitializer
from aggregate_functions import *
from api.where_parser import parse_where_simple
import constants
from utilities.empty_functions import empty_where_function


def get_target(json_dict: dict) -> str:
    if query_type(json_dict) in ['CREATE SOURCE', 'DROP SOURCE']:
        return constants.MAIN_PROCESS_NAME
    if query_type(json_dict) in ['CREATE VIEW, DROP VIEW']:
        return 'source.' + view_source_name(json_dict)
    else:
        return 'view.' + name(json_dict)

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
    ...



def aggregate_initializers(json_dict: dict) -> list[AggregateInitializer]:
    result = []
    for agg_initializer_dict in json_dict['aggregates']:
        function_name = agg_initializer_dict['function']
        column_name = agg_initializer_dict['column']
        parameters = agg_initializer_dict['parameters'] if 'parameters' in agg_initializer_dict else {}
        function_type = utilities.reflection.str_to_type(function_name)
        if not issubclass(function_type, Aggregate):
            raise ValueError  # TODO do something better than exception
        agg_initializer = AggregateInitializer(column_name, function_type, parameters)
        result.append(agg_initializer)
    return result


def send_response(response, request_dict: dict, response_dict: dict, success=True):
    success_str = '1' if success else 0
    request_uuid = request_dict['request_uuid']
    response_dict[request_uuid] = success_str + response


def where(json_dict: dict):
    # TODO
    return empty_where_function #parse_where_simple(json_dict['where'], )


def error_decorator(func):
    def wrapper(request_dict: dict, responses_dict: dict, *args, **kwargs):
        try:
            func(request_dict, responses_dict, *args, **kwargs)
        except Exception as e:
            send_response(str(e), request_dict, responses_dict, success=False)
    return wrapper