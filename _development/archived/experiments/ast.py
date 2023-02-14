import ast

import astpretty

f = '''a > 10 and b < 15 or a > 1000'''


astpretty.pprint(ast.parse(f))
#ast.Module
