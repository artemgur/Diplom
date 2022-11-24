# TODO change to namespace package?
# With that, user would be able to add their own folders with aggregate functions
# To do that, I need to find something that works like __init__.py for namespace packages
# Because without that user would have to use filenames to import any aggregate function

from .base import Aggregate
from .sum import Sum
from .avg import Avg
from .max import Max


# TODO move the function somewhere?
# TODO it doesn't return the base class name, which is fine. Check, does it return subclasses of subclasses or not
# TODO cache return value? The list of aggregate functions won't change at runtime?
def get_aggregate_function_names():
    return list(map(lambda x: x.function_name(), Aggregate.__subclasses__()))
    # return list(filter(lambda x: x.is_function, map(lambda x: x.function_name(), Aggregate.__subclasses__())))
