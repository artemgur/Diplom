def print_response(r, prefix=None):
    """
    Pretty-prints response of requests library in following format:

    status_code content
    :param r: response of requests library
    :param prefix: optional prefix string
    """
    if prefix is not None:
        print(prefix, r.status_code, r.content.decode())
    else:
        print(r.status_code, r.content.decode())


def print_tables_side_by_side(table1: str, table2: str, header1: str, header2: str, spaces_between_tables=5):
    """
    Prints two tabulate tables side by side with header strings.

    :param table1: first table
    :param table2: second table
    :param header1: header string of first table
    :param header2: header string of second table
    :param spaces_between_tables: number of spaces between tables
    """
    split1 = table1.split('\n')
    split2 = table2.split('\n')
    merged = '\n'.join(map(lambda x: x[0] + ' ' * spaces_between_tables + x[1], zip(split1, split2)))
    header = header1.ljust(len(split1[0])) + ' ' * spaces_between_tables + header2.ljust(len(split2[0])) + '\n'
    print(header + merged)
