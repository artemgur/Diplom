# # import aggregate_functions
# #
# # print(aggregate_functions.get_aggregate_function_names())
#
# from aggregate_functions import Avg, Max, Aggregate
#
# print(Avg.needs_column_cache())
# print(Max.needs_column_cache())
# print(Aggregate.needs_column_cache())
# #print(Avg.remove_value == Aggregate.remove_value)
# #print(Max.remove_value == Aggregate.remove_value)

if __name__ == '__main__':
    import processes.main_process

    processes.main_process.run()