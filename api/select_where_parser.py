import re

#from utilities.reflection import is_valid_identifier

function_str = '''
def _parse_where_simple_inner(*args, **kwargs):
    return {0}
'''


def find_all_char(s, substr_regex):
    return list(map(lambda x: x.start(), re.finditer(substr_regex, s)))


# def double_quotes_not_in_strings(double_quotes, strings):
#     current_string = 0
#     for quote_index in double_quotes:
#         while quote_index >= strings[current_string][0]:
#             current_string += 1
#         if current_string >= len(strings):
#             break
#         if quote_index < strings[current_string][0]:
#             yield quote_index

def slice_closed(s, slice_start, slice_end):
    return s[slice_start:slice_end + 1]

def replace_strings_with_spaces(where: str, strings):
    str_builder = []
    current_index = 0
    for string in strings:
        str_builder.append(slice_closed(where, current_index, string[0]))
        spaces_count = string[1] - string[0] - 1
        str_builder.append(' ' * spaces_count)
        current_index = string[1]
    str_builder.append(where[current_index:])
    return ''.join(str_builder)


def list_to_pairs(l):
    return list(zip(l[::2], l[1::2]))

# TODO improve
def replace_identifiers(where: str, where_without_strings: str, columns: list[str], regex_bounds):
    for i, column in enumerate(columns):
        new_str = f'args[{i}]'
        quoted_columns = list(map(lambda x: (x.start(), x.end()), re.finditer(rf'{regex_bounds}{re.escape(column)}{regex_bounds}', where_without_strings)))
        current_index = 0
        where_new = ''
        where_without_strings_new = ''
        for quoted_column in quoted_columns:
            where_new += where[current_index:quoted_column[0]]
            where_new += new_str
            where_without_strings_new += where_without_strings[current_index:quoted_column[0]]
            where_without_strings_new += new_str
            current_index = quoted_column[1]
        where_new += where[current_index:]
        where_without_strings_new += where_without_strings[current_index:]
        where = where_new
        where_without_strings = where_without_strings_new
    return where, where_without_strings

def replace_arguments(where: str, columns: list[str]):
    # Find strings
    escaped_single_quotes = list(map(lambda x: x.end() - 1, re.finditer(r"(?<!\\)(\\\\)*\\'", where)))
    single_quotes = find_all_char(where, "'")
    non_escaped_single_quotes = list(filter(lambda x: x not in escaped_single_quotes, single_quotes)) # Can be optimized, fine for now
    strings = list_to_pairs(non_escaped_single_quotes)
    #print(strings)

    where_without_strings = replace_strings_with_spaces(where, strings)
    #print(where_without_strings)
    where, where_without_strings = replace_identifiers(where, where_without_strings, columns, regex_bounds='"')
    where, where_without_strings = replace_identifiers(where, where_without_strings, columns, regex_bounds=r'\b')
    return where

    #double_quotes = find_all(where, "")
    #quoted_identifier_quotes = double_quotes_not_in_strings(double_quotes, strings)
    #quoted_identifiers = list_to_pairs(quoted_identifier_quotes)

    #for i in escaped_single_quotes:
    #    print(where[i])


def parse(where: str, columns: list[str]):
    where = replace_arguments(where, columns)
    result_function_str = function_str.format(where)
    print(result_function_str)
    exec(result_function_str)
    return locals().get('_parse_where_simple_inner')


#s = r"fghdf''jgch'hsdfegwr\'flfy\\'lmko'u'lm_l_"
# s = "a > 15 or b == 'sgas' and \"sum(b)\" < a"
# print(s)
# print(parse(s, ['a', 'b', 'sum(b)']))