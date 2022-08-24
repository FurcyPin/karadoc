from airflow.models import BaseOperator


def is_trigger(task: BaseOperator) -> bool:
    return task.task_type == "TriggerDagRunOperator"


def is_external_sensor(task: BaseOperator) -> bool:
    return task.task_type == "ExternalTaskSensor"
