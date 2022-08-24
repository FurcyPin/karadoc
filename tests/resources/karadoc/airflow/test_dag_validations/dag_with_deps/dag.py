from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from deps.common import SCHEDULE_INTERVAL

with DAG(
    "dag_with_deps", catchup=False, schedule_interval=SCHEDULE_INTERVAL, start_date=datetime(2017, 6, 23)
) as triggered:
    start = DummyOperator(task_id="start")
