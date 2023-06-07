# from .base import Aggregate
# from .sum import Sum
# from .avg import Avg
# from .max import Max
# from .min import Min

import utilities.class_loader

from .base import Aggregate


utilities.class_loader.load(__file__, __name__, globals(), Aggregate)



# move the function somewhere?
# it doesn't return the base class name, which is fine. Check, does it return subclasses of subclasses or not
# cache return value? The list of aggregate functions won't change at runtime?
# def get_aggregate_function_names():
#     return list(map(lambda x: x.function_name(), Aggregate.__subclasses__()))
#     # return list(filter(lambda x: x.is_function, map(lambda x: x.function_name(), Aggregate.__subclasses__())))
