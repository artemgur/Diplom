import sqlglot.expressions as se


# Returns list of column names
def parse_groupby(groupby_tree: se.Group):
    if groupby_tree is None:
        return None
    return list(map(lambda x: x.this, groupby_tree.find_all(se.Identifier)))