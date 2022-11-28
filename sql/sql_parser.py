import sqlglot
import sqlglot.expressions as se

from sources import base
import where_parser

import select_columns_parser
from orderby import OrderBy
import orderby_parser
from sql import groupby_parser
from table_cache_factory import table_cache_factory







def parse(sql_str: str):
    sql_tree_root = sqlglot.parse(sql_str, 'postgres')[0]

    result_select = select_columns_parser.parse_select_columns(sql_tree_root)

    table_name: str = sql_tree_root.find(se.From).find(sqlglot.expressions.Identifier).this  # None input for FROM not supported
    result_source = source.sources[table_name]
    result_where_callable = where_parser.parse_where_simple(sql_tree_root.find(se.Where))  # TODO improve
    result_orderby = OrderBy(*orderby_parser.parse_orderby(sql_tree_root.find(se.Order)))
    result_groupby = groupby_parser.parse_groupby(sql_tree_root.find(se.Group))

    #check_select_groupby_validity(result_select, result_groupby)
    table_cache_factory(select_columns_result=result_select,
                        source_name=result_source,
                        where_callable=result_where_callable,
                        orderby_result=result_orderby,
                        groupby_result=result_groupby)
