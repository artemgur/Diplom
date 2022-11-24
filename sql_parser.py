from typing import Tuple

import sqlglot
import sqlglot.expressions as se

import aggregate_functions
import source
import where_parser

import select_columns_parser
from orderby import OrderBy
import orderby_parser
import groupby_parser


def check_select_groupby_validity(select, groupby):
    select_columns = filter(lambda x: type(x) is not Tuple, select)
    select_functions = filter(lambda x: type(x) is Tuple, select)
    if set(select_columns) != set(groupby):  # Should sets be used here?
        raise ValueError()  # TODO error message
    valid_aggregate_functions = aggregate_functions.get_aggregate_function_names()
    for select_function in select_functions:
        if select_function[0] not in valid_aggregate_functions:
            raise ValueError(f"Aggregate function {select_function[0]} doesn't exist")




def parse(sql_str: str):
    sql_tree_root = sqlglot.parse(sql_str, 'postgres')[0]

    result_select = select_columns_parser.parse_select_columns(sql_tree_root)

    table_name: str = sql_tree_root.find(se.From).find(sqlglot.expressions.Identifier).this  # None input for FROM not supported
    result_source = source.sources[table_name]
    result_where_callable = where_parser.parse_where_simple(sql_tree_root.find(se.Where))  # TODO improve
    result_orderby = OrderBy(*orderby_parser.parse_orderby(sql_tree_root.find(se.Order)))
    result_groupby = groupby_parser.parse_groupby(sql_tree_root.find(se.Group))

    check_select_groupby_validity(result_select, result_groupby)
