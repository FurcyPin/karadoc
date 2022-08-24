from enum import IntEnum
from typing import Any, Iterable, List

from termcolor import colored


class ValidationSeverity(IntEnum):
    Critical = 5
    Error = 4
    Warning = 3
    Info = 2
    Debug = 1

    @property
    def repr_color(self) -> str:
        """Color used to represent this severity"""
        return ValidationSeverityColorMap[self]

    @property
    def repr_item(self) -> str:
        """Item name used to represent this severity"""
        return ValidationSeverityStringMap[self]

    def decrease(self) -> "ValidationSeverity":
        """Return the severity one level below this one"""
        return ValidationSeverity(max(self.value - 1, 1))

    def increase(self) -> "ValidationSeverity":
        """Return the severity one level above this one"""
        return ValidationSeverity(min(self.value + 1, 5))


ValidationSeverityColorMap = {
    ValidationSeverity.Critical: "red",
    ValidationSeverity.Error: "red",
    ValidationSeverity.Warning: "yellow",
    ValidationSeverity.Info: "white",
    ValidationSeverity.Debug: "white",
}

ValidationSeverityStringMap = {
    ValidationSeverity.Critical: "critical error",
    ValidationSeverity.Error: "error",
    ValidationSeverity.Warning: "warning",
    ValidationSeverity.Info: "info item",
    ValidationSeverity.Debug: "debug item",
}


class ValidationResult:
    def __init__(self, check_type: str, message: str, default_severity: ValidationSeverity):
        self.check_type = check_type
        self.message = message
        self.severity = default_severity

    def __str__(self) -> str:
        return f"{self.severity.name}: {self.check_type}: {self.message}"

    def __repr__(self) -> str:
        return f'ValidationResult("{self.check_type}", "{self.message}", ValidationSeverity.{self.severity.name})'

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ValidationResult):
            return (self.check_type, self.message, self.severity) == (other.check_type, other.message, other.severity)
        else:
            return NotImplemented

    def color_str(self) -> str:
        return colored(self.__str__(), self.severity.repr_color)


class ValidationResultTemplate:
    """Template class for a Validation result.

    Example:
    >>> template = ValidationResultTemplate("karadoc.demo", "this is a {arg}", ValidationSeverity.Info)
    >>> template(arg="demo")
    ValidationResult("karadoc.demo", "this is a demo", ValidationSeverity.Info)

    """

    def __init__(self, check_type: str, message_template: str, default_severity: ValidationSeverity):
        self.check_type = check_type
        self.message_template = message_template
        self.default_severity = default_severity

    def __call__(self, **kwargs: Any) -> ValidationResult:
        return ValidationResult(self.check_type, self.message_template.format(**kwargs), self.default_severity)

    def __str__(self) -> str:
        return f"{self.default_severity.name}: {self.message_template}"

    def __repr__(self) -> str:
        return (
            f"ValidationResultTemplate("
            f'"{self.check_type}", "{self.message_template}", ValidationSeverity.{self.default_severity.name})'
        )


class ValidationException(Exception):
    def __init__(self, results: List[ValidationResult], severity: ValidationSeverity) -> None:
        plural = "s" if len(results) > 1 else ""
        msg = f"Found the following {severity.repr_item}{plural}:\n" + "\n".join([str(r) for r in results])
        Exception.__init__(self, msg)


def fail_if_results(
    results: Iterable[ValidationResult], severity: ValidationSeverity = ValidationSeverity.Error
) -> None:
    """Raise an exception if the given list contains at least one result of the given severity or higher.

    :param results:
    :param severity:
    :return:
    """
    results = [r for r in results if severity is None or r.severity >= severity]
    if len(results) > 0:
        raise ValidationException(results, severity)
