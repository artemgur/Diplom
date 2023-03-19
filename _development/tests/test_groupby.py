from aggregate_functions import Sum
from aggregate_initializer import AggregateInitializer
from groupby import Groupby

from test_utilities.debug import print_separator

groupby = Groupby('view1', groupby_columns=['a'], aggregate_initializers=[AggregateInitializer('b', Sum)])
print('  a b')
while True:
    action, a, b = input().split(' ')
    match action:
        case 'a':
            groupby.insert({'a': int(a), 'b': int(b)})
        case 'r':
            groupby.delete({'a': int(a), 'b': int(b)})
    print_separator()
    for row in groupby.get_rows():
        print(row)
    print_separator()
