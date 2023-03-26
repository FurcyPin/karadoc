import inspect
from types import FunctionType
from typing import Callable, Optional


def method_signature_str(method_name: str, full_arg_spec: inspect.FullArgSpec) -> str:
    annotations = full_arg_spec.annotations

    def arg_str(arg_name: str) -> str:
        if arg_name in annotations:
            return f"{arg_name}: {annotations[arg_name]}"
        else:
            return arg_name

    args = [arg_str(arg_name) for arg_name in full_arg_spec.args]
    return_str = f" -> {annotations['return']}" if "return" in annotations else ""
    return f"def {method_name}({', '.join(args)}){return_str}:"


def check_method_signatures(method_name: str, actual: Callable, expected: Callable) -> None:
    """Ensures that the signature of a given method matches the expected signature taken from another method.

    :param method_name: name of the method to check
    :param actual: function to check
    :param expected: control function with the expected signature
    :return:
    """
    if not isinstance(actual, FunctionType):
        raise TypeError("%s is not a function" % method_name)
    actual_args = inspect.getfullargspec(actual)
    expected_args = inspect.getfullargspec(expected)
    if actual_args != expected_args:
        raise TypeError(
            f"The method {method_name} should have the following signature\n"
            + method_signature_str(method_name, expected_args)
        )


class OptionalMethod:
    def __init__(self, default_func: Callable, name: Optional[str] = None):
        self.func = default_func
        if name is None:
            name = default_func.__name__
        self.name = name

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def set_method(self, func: Callable):
        check_method_signatures(self.name, func, self.func)
        self.func = func
