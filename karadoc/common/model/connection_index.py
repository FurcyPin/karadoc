from typing import Dict, Iterator, Optional

from pyspark.sql import DataFrame

from karadoc.common import conf
from karadoc.common.conf import CONNECTION_GROUP
from karadoc.common.model import file_index
from karadoc.common.table_utils import populate_exists, stream_file_exists
from karadoc.spark.batch.exec import load_populate
from karadoc.spark.stream.exec import load_stream_file


def describe_connection(job, connection_name, source_details, full_table_name, direction, env: Optional[str]):
    try:
        conn = conf.get_connection_conf(connection_name, env)
        common_args = {
            "job_type": type(job).__name__,
            "full_table_name": full_table_name,
            "conn_name": connection_name,
            "conn_type": conn["type"],
            "direction": direction,
            "job_disabled": str(job.disable),
            "conn_disabled": str(conn.get("disable", False)),
        }
        if conn["type"].endswith(".jdbc"):
            return {
                **common_args,
                "host": conn["host"],
                "username": conn["user"],
                "database": conn["database"],
                "table": source_details.get("table"),
            }
        elif conn["type"].endswith(".mssql"):
            return {
                **common_args,
                "host": conn["host"],
                "database": conn["database"],
                "table": source_details.get("table"),
            }
        elif conn["type"].endswith(".cosmosdb"):
            return {
                **common_args,
                "host": conn["host"],
                "database": source_details.get("database"),
                "table": source_details.get("collection"),
            }
        elif conn["type"].endswith(".wasbs"):
            return {
                **common_args,
                "host": conn["storage_account"],
                "container": conn["container_name"],
                "table": source_details.get("path"),
            }
        elif conn["type"].endswith(".azure_table"):
            return {
                **common_args,
                "host": conn["storage_account"],
                "table": source_details.get("table"),
            }
        elif conn["type"].endswith(".event_hub"):
            return {
                **common_args,
                "host": conn["HubName"],
                "username": source_details.get("consumer_group"),
                "table": source_details.get("entity_path"),
            }
        elif conn["type"].endswith(".sftp"):
            return {
                **common_args,
                "host": conn["host"],
                "username": conn["username"],
            }
        elif conn["type"].endswith(".bigquery") or conn["type"].endswith(".bigquery_2"):
            return {
                **common_args,
                "container": conn["bucket_name"],
                "database": conn["project_name"],
                "table": source_details.get("table"),
            }
        else:
            return common_args
    except Exception as e:
        return {
            "full_table_name": full_table_name,
            "conn_name": connection_name,
            "conn_type": "error",
            "direction": direction,
            "host": str(e),
            "disabled": str(job.disable),
        }


def __build_connections_generator(env: Optional[str]) -> Iterator[Dict[str, str]]:  # NOSONAR
    # TODO: refactor this method by making a generic load_action_files method
    for (schema, table, path) in list(file_index.list_schema_table_folders()):
        full_table_name = "%s.%s" % (schema, table)
        if populate_exists(full_table_name):
            job = load_populate(full_table_name)
            for source_name, source_details in job.external_inputs.items():
                connection_name = source_details[CONNECTION_GROUP]
                yield describe_connection(
                    job, connection_name, source_details, full_table_name, direction="input", env=env
                )
            for dest_name, dest_details in job.external_outputs.items():
                connection_name = dest_details[CONNECTION_GROUP]
                yield describe_connection(
                    job, connection_name, dest_details, full_table_name, direction="output", env=env
                )
        if stream_file_exists(full_table_name):
            job = load_stream_file(full_table_name)
            for source_name, source_details in job.external_inputs.items():
                connection_name = source_details[CONNECTION_GROUP]
                yield describe_connection(
                    job, connection_name, source_details, full_table_name, direction="input", env=env
                )
            if job.external_output is not None:
                yield describe_connection(
                    job,
                    job.external_output[CONNECTION_GROUP],
                    job.external_output,
                    full_table_name,
                    direction="output",
                    env=env,
                )


def list_connections_df(env: Optional[str]) -> DataFrame:
    """Builds a Spark DataFrame listing all external inputs/outputs used by POPULATE.py files, with their respective
    source/dest and connection properties.

    :param env: Retrieves the connections properties for the specified environment
    :return: a Spark DataFrame
    """
    from karadoc.spark.batch.spark_batch_job import SparkBatchJob

    job = SparkBatchJob()
    job.init()
    data = __build_connections_generator(env)
    schema = """
        full_table_name: STRING,
        job_type: STRING,
        conn_name: STRING,
        conn_type: STRING,
        direction: STRING,
        host: STRING,
        username: STRING,
        container: STRING,
        database: STRING,
        table: STRING,
        job_disabled: STRING,
        conn_disabled: STRING
    """
    df = job.spark.createDataFrame(data, schema=schema)
    df = df.orderBy("full_table_name")
    return df
