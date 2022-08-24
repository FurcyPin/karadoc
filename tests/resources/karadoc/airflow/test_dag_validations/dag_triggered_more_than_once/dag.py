from datetime import datetime

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator

with DAG("triggered", catchup=False, schedule_interval=None, start_date=datetime(2017, 6, 23)) as triggered:
    start = DummyOperator(task_id="start")

with DAG("trigger_1", catchup=False, start_date=datetime(2017, 6, 23)) as trigger_1:
    TriggerDagRunOperator(trigger_dag_id="triggered", task_id="trigger")

with DAG("trigger_2", catchup=False, start_date=datetime(2017, 6, 23)) as trigger_2:
    TriggerDagRunOperator(trigger_dag_id="triggered", task_id="trigger")
