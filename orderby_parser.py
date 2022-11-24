import sqlglot.expressions as se

from orderby_mode import OrderByMode


# Doesn't parse expressions in ORDER BY
def parse_orderby(orderby_tree: se.Order):
    # TODO None input
    column_names = []
    orderby_modes = []

    for orderby in orderby_tree.find_all(se.Ordered):
        column_name: str = orderby.find(se.Identifier).this
        column_names.append(column_name)

        orderby_mode = OrderByMode(desc=orderby.args['desc'], nulls_first=orderby.args['nulls_first'])
        orderby_modes.append(orderby_mode)

    return column_names, orderby_modes