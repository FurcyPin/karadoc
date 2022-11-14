from enum import Enum
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class CheckSeverity(str, Enum):
    Critical = "Critical"
    Error = "Error"
    Warning = "Warning"
    Info = "Info"
    Debug = "Debug"


class Check:
    global_check_counter = 0

    def __init__(self, func: Callable, description: str, severity: str, name: Optional[str] = None):
        self.func = func
        if name is None:
            self.name = func.__name__
        else:
            self.name = name
        self.description = description
        self.severity = severity

        Check.global_check_counter += 1
        self._creation_rank = Check.global_check_counter
        """Incremental id used to sort the checks in the order they were declared in the QUALITY_CHECK.py file.

        WARNING: When looking at check results of multiple evaluation, the only guarantee is that if you sort
        all the checks with the same table and evaluation_date, they will be sorted in the same order as in the
        QUALITY_CHECK.py file.
        However, there is no guarantee that the id of a given check will remain the same across time.
        """

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


class Alert(Check):
    pass


class Metric(Check):
    pass


def alert(description: str, severity: str, name=None) -> Callable[[Callable[[], "DataFrame"]], Alert]:
    def decorator(func: Callable[[], "DataFrame"]) -> Alert:
        return Alert(func, description, severity, name)

    return decorator


def metric(description: str, severity: str, name=None) -> Callable[[Callable[[], "DataFrame"]], Metric]:
    def decorator(func: Callable[[], "DataFrame"]) -> Metric:
        return Metric(func, description, severity, name)

    return decorator
