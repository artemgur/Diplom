def are_lists_equal(list1: list, list2: list):
    for i in range(len(list1)):
        if not list1[i] == list2[i]:
            return False
    return True


def find_multiple(l: list, objects_to_find: list):
    result = []
    for obj in objects_to_find:
        result.append(l.index(obj))
    return result


def index_many(l: list, indexes: list):
    result = []
    for index in indexes:
        result.append(l[index])
    return result