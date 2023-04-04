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


def difference(a: list, b: list):
    return list(set(a) - set(b))


# def pad_left(l: list, padding_element, pad_length: int):
#     new_elements_count = max(pad_length - len(l), 0)
#     return [padding_element] * new_elements_count + l