# import aggregate_functions
#
# print(aggregate_functions.get_aggregate_function_names())

from aggregate_functions import Avg, Max, Aggregate

print(Avg.has_delete())
print(Max.has_delete())
print(Aggregate.has_delete())
#print(Avg.remove_value == Aggregate.remove_value)
#print(Max.remove_value == Aggregate.remove_value)