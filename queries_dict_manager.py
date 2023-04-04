def add(queries_dict: dict, key, value):
    if key in queries_dict:
        queries = queries_dict[key]
        queries.append(value)
        queries_dict[key] = queries
    else:
        queries_dict[key] = [value]