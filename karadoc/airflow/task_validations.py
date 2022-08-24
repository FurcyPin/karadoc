from typing import Dict, Iterable, List, Set

import networkx as nx
from airflow.models import BaseOperator, DagBag

from karadoc.airflow.dag_graph import task_full_name
from karadoc.common.model.table_index import Populate, Table
from karadoc.common.validations import ValidationResult, ValidationSeverity


def is_karadoc_run_task(task: BaseOperator):
    return task.task_type == "KaradocSubmitRunOperator"


def validate_dependencies(
    task: BaseOperator,
    inputs: List[str],
    transitive_closure_graph: nx.DiGraph,
    scheduled_tables: Set[str],
    table_index: Dict[str, Table],
    dag_bag: DagBag,
) -> List[ValidationResult]:
    """Verifies that the dependencies of the Populate are respected by the DAG.
    For that we check that each input is a parent of this table in the transitive closure of the DAG.

    :param task: The airflow task currently being inspected
    :param inputs: List of input table full names declared in the POPULATE.py (job.inputs)
    :param transitive_closure_graph: A dag where the parents of each task are all its ancestors
    :param scheduled_tables: The list of all tables that scheduled in Airflow
    :param table_index: A dict (full_table_name -> table information)
    :param dag_bag: Airflow's DagBag that contains the list of dags.
    :return: a list of ValidationResults
    """

    def task_id_list_to_table_set(nodes):
        def gen():
            for n in nodes:
                dag_id, task_id = n.split(".", maxsplit=1)
                task = dag_bag.dags[dag_id].get_task(task_id)
                if is_karadoc_run_task(task):
                    yield task.table

        return set(gen())

    active_inputs = [input for input in inputs if not table_index[input].populate.disabled]
    ancestor_nodes = list(transitive_closure_graph.predecessors(task_full_name(task)))
    ancestor_tables = task_id_list_to_table_set(ancestor_nodes)
    same_dag_tables = [task.table for task in task.dag.tasks if is_karadoc_run_task(task)]

    res = []
    for input in active_inputs:
        if input not in scheduled_tables:
            msg = "DAG {dag} : Table {table} : the input {input} does not appear in any DAG ".format(
                dag=task.dag_id, table=task.table, input=input
            )
            res.append(ValidationResult("karadoc.airflow.dependency_not_in_any_dag", msg, ValidationSeverity.Error))
        elif input in same_dag_tables and input not in ancestor_tables:
            msg = "DAG {dag} : Table {table} : the input {input} appears in the same DAG but is not upstream ".format(
                dag=task.dag_id, table=task.table, input=input
            )
            res.append(
                ValidationResult(
                    "karadoc.airflow.dependency_in_same_dag_but_not_upstream", msg, ValidationSeverity.Error
                )
            )
        elif input in scheduled_tables and input not in ancestor_tables:
            msg = "DAG {dag} : Table {table} : the input {input} does not appear in any upstream DAG ".format(
                dag=task.dag_id, table=task.table, input=input
            )
            res.append(ValidationResult("karadoc.airflow.dependency_not_upstream", msg, ValidationSeverity.Warning))
    return res


def validate_vars(task, populate: Populate) -> List[ValidationResult]:
    """Check that the there is no discrepancy between the vars passed to the KararadocOperator and the vars
    declared in the populate file.
    """
    res = []
    if task.vars and not populate.vars:
        msg = "DAG {dag} : Table {table} requires no vars but is given {given_vars}".format(
            dag=task.dag_id, table=task.table, given_vars=list(task.vars.keys())
        )
        result = ValidationResult("karadoc.airflow.unused_vars", msg, ValidationSeverity.Warning)
        res.append(result)
    if populate.vars and not task.vars:
        msg = "DAG {dag} : Table {table} requires vars {required_vars} but none is given".format(
            dag=task.dag_id, table=task.table, required_vars=list(populate.vars.keys())
        )
        result = ValidationResult("karadoc.airflow.missing_vars", msg, ValidationSeverity.Error)
        res.append(result)
    if task.vars and populate.vars and task.vars.keys() != populate.vars.keys():
        msg = "DAG {dag} : Table {table} requires vars {required_vars} but is given {given_vars}".format(
            dag=task.dag_id,
            table=task.table,
            required_vars=list(populate.vars.keys()),
            given_vars=list(task.vars.keys()),
        )
        result = ValidationResult("karadoc.airflow.vars_mismatch", msg, ValidationSeverity.Error)
        res.append(result)
    return res


def validate_karadoc_table(task: BaseOperator, table_index, **kwargs) -> List[ValidationResult]:
    """Perform various verifications on Karadoc Tables

    We verify that the Populate file exists, that it is not disabled
    and that the inputs of the table are located upstream in the Airflow DAGs.
    """
    res = []
    if is_karadoc_run_task(task):
        table = table_index.get(task.table)
        if table is None:
            msg = "DAG {dag} : Table {table} does not exists".format(dag=task.dag_id, table=task.table)
            result = ValidationResult("karadoc.table.missing", msg, ValidationSeverity.Error)
            return [result]
        if table.populate is None:
            msg = "DAG {dag} : Table {table} has no Populate".format(dag=task.dag_id, table=task.table)
            result = ValidationResult("karadoc.table.missing_populate", msg, ValidationSeverity.Error)
            return [result]
        if table.populate.disabled:
            msg = "DAG {dag} : Table {table} is disabled".format(dag=task.dag_id, table=task.table)
            result = ValidationResult("karadoc.table.disabled", msg, ValidationSeverity.Error)
            return [result]

        res += validate_vars(task, table.populate)
        res += validate_dependencies(task, table.populate.input_tables, table_index=table_index, **kwargs)

    return res


validations = [
    validate_karadoc_table,
]
"""List of validation methods that will be called for each Airflow Task"""


def validate_task(task: BaseOperator, **kwargs) -> Iterable[ValidationResult]:
    for validation in validations:
        yield from validation(task, **kwargs)


def validate_all_tasks(
    all_tasks: Iterable[BaseOperator],
    dag_bag: DagBag,
    scheduled_tables: Set[str],
    table_index: Dict[str, Table],
    transitive_closure_graph: nx.DiGraph,
) -> Iterable[ValidationResult]:
    for task in all_tasks:
        yield from validate_task(
            task,
            dag_bag=dag_bag,
            transitive_closure_graph=transitive_closure_graph,
            scheduled_tables=scheduled_tables,
            table_index=table_index,
        )
