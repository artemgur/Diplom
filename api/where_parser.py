import re

from utilities.reflection import is_valid_identifier


def find_all(s, substr_regex):
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
    return s[slice_start:slice_end - 1]

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


def replace_identifier(where: str, where_without_spaces: str, identifier: str, args_number: int):
    ...


#def find_identifiers(where: str, columns: list[str]):
#    for identifier in columns:


def list_to_pairs(l):
    return list(zip(l[::2], l[1::2]))


def parse(where: str, columns: list[str]):
    # Find strings
    escaped_single_quotes = list(map(lambda x: x.end() - 1, re.finditer(r"(?<!\\)(\\\\)*\\'", where)))
    single_quotes = find_all(where, "'")
    non_escaped_single_quotes = list(filter(lambda x: x not in escaped_single_quotes, single_quotes)) # Can be optimized, fine for now
    strings = list_to_pairs(non_escaped_single_quotes)
    print(strings)

    where_without_strings = replace_strings_with_spaces(where, strings)

    #double_quotes = find_all(where, "")
    #quoted_identifier_quotes = double_quotes_not_in_strings(double_quotes, strings)
    #quoted_identifiers = list_to_pairs(quoted_identifier_quotes)

    #for i in escaped_single_quotes:
    #    print(where[i])


s = r"'hsdfegwr\'flfy\\''u'"
print(s)
parse(s, [])