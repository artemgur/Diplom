import re


# Source: https://stackoverflow.com/a/1176023
to_snake_case_regex = re.compile(r'(?<!^)(?=[A-Z])')


def to_snake_case(string):
    return to_snake_case_regex.sub('_', string).lower()


def classname_to_snake_case(classname: type):
    return to_snake_case(classname.__name__)
