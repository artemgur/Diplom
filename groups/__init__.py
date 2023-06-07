import utilities.class_loader

from .group import Group


utilities.class_loader.load(__file__, __name__, globals(), Group)
