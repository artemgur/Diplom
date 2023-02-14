import keyword

# noinspection PyUnresolvedReferences
from sources import *
# noinspection PyUnresolvedReferences
from aggregate_functions import *


def str_to_type(x: str):
    if not is_valid_identifier(x):
        raise ValueError(f'String "{x}" is not a valid identifier')
    result = eval(x)
    if not isinstance(result, type):
        raise ValueError(f'String "{x}" is not a valid type')
    return result


# Source: https://stackoverflow.com/a/29586366
def is_valid_identifier(ident: str) -> bool:
    """Determines if string is valid Python identifier."""

    if not isinstance(ident, str):
        raise TypeError("expected str, but got {!r}".format(type(ident)))

    if not ident.isidentifier():
        return False

    if keyword.iskeyword(ident):
        return False

    return True