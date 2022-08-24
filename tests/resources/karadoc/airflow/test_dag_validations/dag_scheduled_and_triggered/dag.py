from datetime import datetime

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator

with DAG("triggered", catchup=False, schedule_interval="0 0 * * *", start_date=datetime(2017, 6, 23)) as triggered:
    start = DummyOperator(task_id="start")

with DAG("trigger", catchup=False, start_date=datetime(2017, 6, 23)) as trigger:
    TriggerDagRunOperator(trigger_dag_id="triggered", task_id="trigger")
