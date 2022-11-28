import sqlglot.expressions as se

from utilities.empty_functions import empty_where_function

function_str = '''
def _parse_where_simple_inner({0}, **kwargs):
    return {1}
'''





# Very simple where condition parser
def parse_where_simple(where_tree: se.Where):
    if where_tree is None:
        return empty_where_function

    where_str = where_tree.sql()
    where_str_prepared = where_str.lower().replace('=', '==')
    identifiers = set(map(lambda x: x.this, where_tree.find_all(se.Identifier)))
    identifier_str = ', '.join(identifiers)
    result_function_str = function_str.format(identifier_str, where_str_prepared)
    print(result_function_str)
    exec(result_function_str)
    return locals().get('_parse_where_simple_inner')



