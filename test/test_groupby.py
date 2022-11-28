from aggregate_functions import Sum
from groupby import Groupby

from utilities.debug import print_separator

groupby = Groupby(groupby_columns='a', agg_list_initializer=[('b', Sum)], column_names=['a', 'b_sum'])
print('  a b')
while True:
    action, a, b = input().split(' ')
    match action:
        case 'a':
            groupby.insert({'a': int(a), 'b': int(b)})
        case 'r':
            groupby.delete({'a': int(a), 'b': int(b)})
    print_separator()
    for row in groupby.get_result():
        print(row)
    print_separator()
