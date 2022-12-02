def are_lists_equal(list1: list, list2: list):
    for i in range(len(list1)):
        if not list1[i] == list2[i]:
            return False
    return True