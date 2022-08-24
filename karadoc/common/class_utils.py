import inspect
from typing import Any, Type

from karadoc.common.exceptions import ConnectorLoadError


def _is_defined_here(obj: Any, module) -> bool:
    """Returns true if the given class was defined in the given module.
    This discards classes that are imported in the module but defined elsewhere."""
    return inspect.getfile(obj) == inspect.getfile(module)


def find_class_from_module(module, class_type: Type[Any]):
    """Locates in the file the class defined that extends the specified class and returns it.
    Raises an exception if more than one such class is found in the module.

    :return:
    """
    class_full_name = class_type.__module__ + "." + class_type.__qualname__
    found_classes = [
        obj
        for name, obj in inspect.getmembers(module)
        if inspect.isclass(obj) and issubclass(obj, class_type) and _is_defined_here(obj, module)
    ]
    if len(found_classes) == 1:
        return found_classes[0]
    if len(found_classes) == 0:
        raise ConnectorLoadError(
            f"Error while loading the module named '{module.__name__}' : "
            f"no class extending {class_full_name} was found"
        )
    if len(found_classes) > 1:
        raise ConnectorLoadError(
            f"Error while loading the module named '{module.__name__}' : "
            f"we found more than one class extending {class_full_name}\n"
            f"Classes found: {found_classes}"
        )
