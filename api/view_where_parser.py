function_str = '''
def _parse_where_simple_inner(**kwargs):
    return {0}
'''


def parse(where: str):
    result_function_str = function_str.format(where)
    print(result_function_str)
    exec(result_function_str)
    return locals().get('_parse_where_simple_inner')
