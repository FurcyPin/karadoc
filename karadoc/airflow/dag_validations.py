from typing import Any, Dict, Generator, Iterable

from airflow.models import DagBag

from karadoc.airflow.utils import is_trigger
from karadoc.common.validations import ValidationResult, ValidationResultTemplate, ValidationSeverity

ValidationResult_DagTriggeredMoreThanOnce = ValidationResultTemplate(
    check_type="karadoc.airflow.dag_triggered_more_than_once",
    message_template="The dag {dag_id} is triggered {count} times",
    default_severity=ValidationSeverity.Warning,
)

ValidationResult_DagNotScheduledNorTriggered = ValidationResultTemplate(
    check_type="karadoc.airflow.dag_not_scheduled_nor_triggered",
    message_template="The dag {dag_id} is never scheduled nor triggered",
    default_severity=ValidationSeverity.Warning,
)

ValidationResult_DagScheduledAndTriggered = ValidationResultTemplate(
    check_type="karadoc.airflow.dag_scheduled_and_triggered",
    message_template="The dag {dag_id} is scheduled and triggered",
    default_severity=ValidationSeverity.Warning,
)


def _element_count(iterable: Iterable[Any]) -> Dict[Any, int]:
    """Count the number of occurrences of each distinct value in a list and returns a Dict[elem, int]

    >>> _element_count([1, 2, 2])
    {1: 1, 2: 2}
    >>> _element_count("abracadabra")
    {'a': 5, 'b': 2, 'r': 2, 'c': 1, 'd': 1}
    """
    res: Dict[Any, int] = dict()
    for item in iterable:
        if item in res:
            res[item] += 1
        else:
            res[item] = 1
    return res


def _get_triggered_dags(dag_bag: DagBag) -> Dict[str, int]:
    """List all dags that are triggered along with the number of times they are triggered

    :return: a Dict(dag_id, number_of_times_triggered)"""

    def list_all() -> Generator[str, None, None]:
        for dag in dag_bag.dags.values():
            for task in dag.tasks:
                if is_trigger(task):
                    yield task.trigger_dag_id

    return _element_count(list_all())


def _get_scheduled_dags(dag_bag: DagBag) -> Generator[str, None, None]:
    """List all dags that are scheduled"""
    for dag in dag_bag.dags.values():
        if dag.schedule_interval is not None:
            yield dag.dag_id


def validate_all_dags(dag_bag: DagBag) -> Generator[ValidationResult, None, None]:
    triggered_dags_with_count = _get_triggered_dags(dag_bag)
    triggered_dags = set(triggered_dags_with_count.keys())
    all_dags = set(dag_bag.dags.keys())
    scheduled_dags = set(_get_scheduled_dags(dag_bag))

    for dag_id, count in triggered_dags_with_count.items():
        if count > 1:
            yield ValidationResult_DagTriggeredMoreThanOnce(dag_id=dag_id, count=count)

    for dag_id in all_dags.difference(triggered_dags).difference(scheduled_dags):
        yield ValidationResult_DagNotScheduledNorTriggered(dag_id=dag_id)

    for dag_id in scheduled_dags.intersection(triggered_dags):
        yield ValidationResult_DagScheduledAndTriggered(dag_id=dag_id)
