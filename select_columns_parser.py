import sqlglot.expressions as se

from utilities.utilities import classname_to_snake_case


def parse_function(function_subtree: se.Func):
    if type(function_subtree) is se.Anonymous:
        return function_subtree.this, function_subtree.expressions[0].this  # Only single parameter for now
    else:
        # TODO check how it works for functions with non-Anonymous classname and multiple parameters, if such functions exist
        return classname_to_snake_case(type(function_subtree)), function_subtree.this.this


# Now only parses column identifiers and aggregate functions with single column identifier as parameter
def parse_select_column(select_subtree: se.Expression):
    if isinstance(select_subtree, se.Func):
        function_name, identifier_name = parse_function(se.Expression)
        return function_name, identifier_name
    else:
        identifier_name = select_subtree.this
        return identifier_name


def parse_select_columns(select_tree: se.Select):
    result = []
    for column in select_tree.expressions:
        result.append(column)
    return result