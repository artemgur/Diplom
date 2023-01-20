import sqlglot.expressions as se

from utilities.empty_functions import empty_where_function

function_str = '''
def _parse_where_simple_inner({0}, **kwargs):
    return {1}
'''





# Very simple where condition parser
def parse_where_simple(where: str, columns: list[str]):
    #identifiers = set(map(lambda x: x.this, where_tree.find_all(se.Identifier)))
    identifier_str = ', '.join(columns)
    result_function_str = function_str.format(identifier_str, where)
    print(result_function_str)
    exec(result_function_str)
    return locals().get('_parse_where_simple_inner')



