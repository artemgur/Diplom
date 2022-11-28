def check_select_groupby_validity(select, groupby):
    select_columns = filter(lambda x: type(x) is not Tuple, select)
    select_functions = filter(lambda x: type(x) is Tuple, select)
    if set(select_columns) != set(groupby):  # Should sets be used here?
        raise ValueError()  # TODO error message
    valid_aggregate_functions = aggregate_functions.get_aggregate_function_names()
    for select_function in select_functions:
        if select_function[0] not in valid_aggregate_functions:
            raise ValueError(f"Aggregate function {select_function[0]} doesn't exist")


def table_cache_factory(select_columns_result, source_name, where_callable, orderby_result, groupby_result):
    select_columns = filter(lambda x: type(x) is not Tuple, select)
    select_functions = filter(lambda x: type(x) is Tuple, select)
    if set(select_columns) != set(groupby):  # Should sets be used here?
        raise ValueError()  # TODO error message
