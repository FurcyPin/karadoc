from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

with DAG("triggered", catchup=False, schedule_interval=None, start_date=datetime(2017, 6, 23)) as triggered:
    start = DummyOperator(task_id="start")
