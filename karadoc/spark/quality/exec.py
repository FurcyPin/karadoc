from io import StringIO
from typing import Dict, Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as f
from spark_frame.functions import empty_array, generic_struct
from termcolor import colored

from karadoc.common.exceptions.utils import display_exception
from karadoc.common.job_core.load import (
    load_non_runnable_action_file,
    load_runnable_action_file,
)
from karadoc.spark.batch import exec
from karadoc.spark.batch.exec import load_runnable_populate
from karadoc.spark.quality.checks import Alert, Metric
from karadoc.spark.quality.quality_check_job import QualityCheckJob


def load_quality_check(full_table_name) -> QualityCheckJob:
    """Return the `job` object declared in the QUALITY_CHECK.py file of the given table with its default variables.

    This method is useful for inspecting a QUALITY_CHECK.py file without running it.
    For running quality checks, please use `load_runnable_quality_check` instead.

    :param full_table_name: Full name of the table (schema_name.table_name)
    :return: the QualityCheckJob object defined in the QUALITY_CHECK.py file
    """
    return load_non_runnable_action_file(full_table_name, QualityCheckJob)


def load_runnable_quality_check(full_table_name, passed_vars: Optional[Dict[str, str]]) -> QualityCheckJob:
    """Return the `job` object declared in the QUALITY_CHECK.py file of the given table, override its variables
    and check the load variables match those declared in the QUALITY_CHECK.py file.

    :param full_table_name: Full name of the table (schema_name.table_name)
    :param passed_vars: Set the variables of the job and check that they match the ones declared
    :return: the QualityCheckJob object defined in the QUALITY_CHECK.py file
    """
    return load_runnable_action_file(full_table_name, QualityCheckJob, passed_vars)


def __create_alert_struct(table: str, alert: Alert, status: str) -> Column:
    return f.struct(
        f.lit(table).alias("table"),
        f.lit(alert.name).alias("name"),
        f.lit(alert.description).alias("description"),
        f.lit(alert.severity).alias("severity"),
        f.lit(f.current_timestamp()).alias("evaluation_date"),
        f.lit(status).alias("status"),
        f.lit(alert._creation_rank).alias("rank"),
    ).alias("alert")


def __alert_ok_dataframe(spark: SparkSession, table: str, alert: Alert) -> DataFrame:
    alert_col = __create_alert_struct(table, alert, status="ok")
    values_col = empty_array("STRUCT<key: STRING, value: STRING>").alias("columns")
    return spark.sql("SELECT 1").select(alert_col, values_col)


def __alert_exception_dataframe(spark: SparkSession, table: str, alert: Alert, e: Exception) -> DataFrame:
    display_exception(e)
    dest = StringIO()
    display_exception(e, dest)
    exception_message = dest.getvalue()
    alert_col = __create_alert_struct(table, alert, status="error")
    values_col = f.array(f.struct(f.lit("exception").alias("key"), f.lit(exception_message).alias("value"))).alias(
        "columns"
    )
    return spark.sql("SELECT 1").select(alert_col, values_col)


def __transform_alert_for_export(table: str, alert: Alert, df: DataFrame) -> DataFrame:
    alert_col = __create_alert_struct(table, alert, status="ko")
    values_col = generic_struct(*df.columns, col_name_alias="key", col_value_alias="value").alias("columns")
    return df.select(alert_col, values_col)


def __print_alert(alert: Alert):
    try:
        df = __compute_alert_dataframe(alert)
        df = df.persist()
        count = df.count()
        if count == 0:
            print(colored("%s: OK\n%s" % (alert.name, alert.description), "green"))
        else:
            print(colored("%s: NOT OK (%s rows in error)\n%s" % (alert.name, count, alert.description), "red"))
            df.show(10, False)
    except Exception as e:
        print(colored("Error running alert: %s" % alert.name, "red"))
        display_exception(e)


def __write_alert(table: str, alert: Alert, output_table: str, vars: Dict[str, str]):
    export_job = load_runnable_populate(output_table, vars)
    export_job.init()
    try:
        df = __compute_alert_dataframe(alert)
        df = df.persist()
        df = __transform_alert_for_export(table, alert, df)
        count = df.count()
        if count == 0:
            print(colored("%s: OK\n%s" % (alert.name, alert.description), "green"))
            df = __alert_ok_dataframe(export_job.spark, table, alert)
        else:
            print(colored("%s: NOT OK (%s rows in error)\n%s" % (alert.name, count, alert.description), "red"))
        exec.write_output(export_job, df)
    except Exception as e:
        print(colored("Error running alert: %s" % alert.name, "red"))
        df = __alert_exception_dataframe(export_job.spark, table, alert, e)
        exec.write_output(export_job, df)


def inspect_alert_dataframe(alert: Alert, df: DataFrame):
    """Function used for unit tests. Mock this function to be able to inspect the produced DataFrame
    during the execution of the command."""
    pass


def __compute_alert_dataframe(alert: Alert) -> DataFrame:
    df = alert.func()
    inspect_alert_dataframe(alert, df)
    if not isinstance(df, DataFrame):
        raise TypeError(
            f"Alert {alert.name} is supposed to return a Spark DataFrame, but returned a {type(df)} instead"
        )
    return df


def execute_alert(table: str, alert: Alert, vars: Dict[str, str], output_table: Optional[str]):
    if output_table is not None:
        __write_alert(table, alert, output_table, vars)
    else:
        __print_alert(alert)


def execute_metric(table: str, metric: Metric, vars: Dict[str, str], output_table: Optional[str]):  # NOSONAR
    # TODO: this feature is not fully implemented yet. We should support output_tables.
    print("metric: %s" % metric.description)
    df = metric.func()
    if not isinstance(df, DataFrame):
        raise TypeError(
            f"Metric {metric.name} is supposed to return a Spark DataFrame, but returned a {type(df)} instead"
        )
    df.show(10, False)
