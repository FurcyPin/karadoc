from enum import IntEnum
from typing import Callable


def _bin_op(op_name: str) -> Callable[["ReturnCode", "ReturnCode"], "ReturnCode"]:
    """Create a method for given binary operator"""

    def func(self: "ReturnCode", other: "ReturnCode") -> "ReturnCode":
        if not isinstance(other, ReturnCode):
            return NotImplemented
        op = getattr(self.value, op_name)
        return ReturnCode(op(other.value))

    return func


class ReturnCode(IntEnum):
    """Enum return code for a karadoc command.

    It's either Success (0) or Error (1)
    It supports & and | operators

    Examples:

    >>> ReturnCode.Success != ReturnCode.Error
    True
    >>> ReturnCode.Success == ReturnCode(0)
    True
    >>> ReturnCode.Error == ReturnCode(1)
    True
    >>> ReturnCode.Success & ReturnCode.Error
    <ReturnCode.Success: 0>
    >>> ReturnCode.Success | ReturnCode.Error
    <ReturnCode.Error: 1>

    """

    Success = 0
    Error = 1

    __or__ = _bin_op("__or__")
    __and__ = _bin_op("__and__")
