from inspect import isclass
from pkgutil import iter_modules
from pathlib import Path
from importlib import import_module


# Source: https://julienharbulot.com/python-dynamical-import.html
def load(file_, name_, globals_, superclass):
    # iterate through the modules in the current package
    package_dir = Path(file_).resolve().parent
    for (_, module_name, _) in iter_modules([package_dir]):

        # import the module and iterate through its attributes
        module = import_module(f"{name_}.{module_name}")
        for attribute_name in dir(module):
            attribute = getattr(module, attribute_name)

            if isclass(attribute) and issubclass(attribute, superclass):
                # Add the class to this package's variables
                globals_[attribute_name] = attribute