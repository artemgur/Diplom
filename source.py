sources = {}  # TODO change from dict to specialized type?


class Source:
    def __init__(self, name):
        self._name = name
        sources[name] = self