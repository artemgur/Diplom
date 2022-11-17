import ast

import astpretty
import sqlglot.expressions as se


f = '''def test_function(a, b):
    return a > 10 and b < 15 or a > 1000'''


def parse_where(where: se.Where):
    parameters = map(lambda x: x.this, where.find_all(se.Identifier))
    function = ast.FunctionDef()

astpretty.pprint(ast.parse(f))
#ast.Module
