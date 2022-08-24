from typing import Dict, List, Set

from airflow.models import BaseOperator, DagBag

from karadoc.airflow.dag_validations import validate_all_dags
from karadoc.airflow.task_validations import is_karadoc_run_task, validate_all_tasks
from karadoc.common.model.table_index import Table
from karadoc.common.validations import ValidationResult, ValidationSeverity


def list_tasks(dag_bag: DagBag) -> List[str]:
    return [task for dag in dag_bag.dags.values() for task in dag.tasks]


def list_scheduled_tables(tasks: List[BaseOperator]) -> Set[str]:
    """Return the list of all tasks that correspond to a KaradocSubmitRunOperator"""
    return {task.table for task in tasks if is_karadoc_run_task(task)}


def validate_dag_bag(dag_bag: DagBag, table_index: Dict[str, Table]) -> List[ValidationResult]:
    from karadoc.airflow.dag_graph import build_graph, transitive_closure

    dag_graph = build_graph(dag_bag)
    transitive_closure_graph = transitive_closure(dag_graph)
    all_tasks = list_tasks(dag_bag)
    scheduled_tables = list_scheduled_tables(all_tasks)
    error_messages = [
        ValidationResult(
            "airflow.import.error",
            "could not load file {file} : {message}".format(file=file, message=message),
            ValidationSeverity.Error,
        )
        for file, message in dag_bag.import_errors.items()
    ]
    error_messages += list(
        validate_all_tasks(all_tasks, dag_bag, scheduled_tables, table_index, transitive_closure_graph)
    )
    error_messages += list(validate_all_dags(dag_bag))
    return sorted(error_messages, key=(lambda e: e.severity.value))
