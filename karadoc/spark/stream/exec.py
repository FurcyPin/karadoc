from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

from karadoc.common import conf
from karadoc.common.job_core.load import (
    load_non_runnable_action_file,
    load_runnable_action_file,
)
from karadoc.common.output.printing import pretty_format
from karadoc.common.table_utils import parse_table_name
from karadoc.spark.job_core.has_spark import _table_name_to_hdfs_path
from karadoc.spark.stream.spark_stream_job import SparkStreamJob


def load_stream_file(full_table_name: str) -> SparkStreamJob:
    """Return a non-runnable version of the `job` object declared in the STREAM.py file of the given table
    with its default variables.

    This method is useful to load any STREAM for inspecting its metadata (input, output, output_partitions, etc.)
    even when no variable is defined, by using its default values. The `stream` method is removed to prevent any misuse
    where one would call the STREAM without setting the variables properly.

    If you want to execute the `stream` method use the `load_runnable_stream_file` method instead.

    :param full_table_name: Full name of the table (schema_name.table_name)
    :return: the SparkBatchJob object defined in the populate.
    """
    return load_non_runnable_action_file(full_table_name, SparkStreamJob)


def load_runnable_stream_file(full_table_name: str, passed_vars: Optional[Dict[str, str]]) -> SparkStreamJob:
    """Return the `job` object declared in the STREAM.py file of the given table, override its variables
    and check the load variables match those declared in the
    :param full_table_name: Full name of the table (schema_name.table_name)
    :param passed_vars: Set the variables of the job and check that they match the ones declared
    :return: the SparkStreamJob object defined in the STREAM.py file
    """
    return load_runnable_action_file(full_table_name, SparkStreamJob, passed_vars)


def print_job_properties(job: SparkStreamJob):
    if job.external_inputs != {}:
        print("external_inputs :")
        print(pretty_format(job.external_inputs))
    if job.external_output is not None:
        print("external_outputs:")
        print(pretty_format(job.external_output))


def get_checkpoint_location(output: str):
    (schema_name, table_name, _) = parse_table_name(output)
    checkpoint_folder_location = conf.get_streaming_checkpoint_folder_location()
    return _table_name_to_hdfs_path(checkpoint_folder_location, schema_name, table_name, None)


def write_stream_output(job: SparkStreamJob, df, no_export: bool, streaming_mode: str, batch_interval: Optional[str]):
    """Serialize the output of a stream if it exists and is not empty, and if its schema is correctly defined.
    By default, the output is written as orc, but future development might allow the user to specify parquet instead.
    """
    if df and type(df) is DataFrame:
        if df.columns:
            if no_export:
                print("WARN: The job export has been disabled.")
            if job.external_output is not None and not no_export:
                ds_writer = job.write_external_output(df, job.external_output)
            else:
                ds_writer = job.write_table(df)
            __start_stream_writer(ds_writer, job, streaming_mode, batch_interval)
        else:
            print("WARN: The job returned a DataFrame with zero columns. The output will not be serialized.")


def __start_stream_writer(ds_writer: DataStreamWriter, job: SparkStreamJob, streaming_mode: str, batch_interval: str):
    checkpoint_location = get_checkpoint_location(job.output)
    ds_writer = ds_writer.option("checkpointLocation", checkpoint_location)
    streaming_modes = ["once", "microbatch"]
    if streaming_mode == "microbatch":
        if batch_interval is None:
            raise ValueError("batch_interval is required for the microbatch mode")
        else:
            ds_writer = ds_writer.trigger(processingTime=batch_interval)
    elif streaming_mode == "once":
        ds_writer = ds_writer.trigger(once=True)
    else:
        raise ValueError(
            f"value streaming_mode: {streaming_mode} is not one of the supported streaming modes : {streaming_modes}"
        )
    ds_writer.start().awaitTermination()
