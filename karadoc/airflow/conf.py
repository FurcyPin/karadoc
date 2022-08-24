import dynaconf

AIRFLOW_GROUP = "airflow"


def get_airflow_dag_folder_location() -> str:
    return dynaconf.settings.get(f"{AIRFLOW_GROUP}.dag_folder", default="airflow/dags")


def get_airflow_plugin_folder_location() -> str:
    return dynaconf.settings.get(f"{AIRFLOW_GROUP}.plugin_folder", default="airflow/plugins")
