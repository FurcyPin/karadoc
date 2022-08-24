from typing import Generator, Iterable, TypeVar

from karadoc.common.quality.quality_check_job import QualityCheckJob
from karadoc.common.validations import (
    ValidationResult,
    ValidationResultTemplate,
    ValidationSeverity,
)

T = TypeVar("T")


ValidationResult_QualityCheckAlertDuplicateName = ValidationResultTemplate(
    check_type="karadoc.quality_check.alert.duplicate_name",
    message_template='alert name "{alert_name_duplicate}" is defined multiple times',
    default_severity=ValidationSeverity.Error,
)

ValidationResult_QualityCheckMetricDuplicateName = ValidationResultTemplate(
    check_type="karadoc.quality_check.metric.duplicate_name",
    message_template='metric name "{metric_name_duplicate}" is defined multiple times',
    default_severity=ValidationSeverity.Error,
)


def __find_duplicate(_list: Iterable[T]) -> T:
    """Return the first duplicate found in the given list

    >>> __find_duplicate([1, 2, 3, 4, 2, 1])
    2
    >>> __find_duplicate([1, 2, 3, 4])

    >>> __find_duplicate([])


    :param _list:
    :return:
    """
    res = set()
    for elem in _list:
        if elem in res:
            return elem
        else:
            res.add(elem)


def validate_alert_duplicate_names(job: QualityCheckJob, **_) -> Generator[ValidationResult, None, None]:
    """Yield errors if the job has two alerts with the same name"""
    alert_name_duplicate = __find_duplicate([alert.name for alert in job.alerts])
    if alert_name_duplicate is not None:
        yield ValidationResult_QualityCheckAlertDuplicateName(alert_name_duplicate=alert_name_duplicate)


def validate_metric_duplicate_names(job: QualityCheckJob, **_) -> Generator[ValidationResult, None, None]:
    """Yield errors if the job has two metrics with the same name"""
    metric_name_duplicate = __find_duplicate([metric.name for metric in job.metrics])
    if metric_name_duplicate is not None:
        yield ValidationResult_QualityCheckMetricDuplicateName(metric_name_duplicate=metric_name_duplicate)
