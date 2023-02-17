from typing import TYPE_CHECKING, Dict, Optional

from karadoc.common.job_core.load import load_non_runnable_action_file, load_runnable_action_file
from karadoc.common.output.printing import pretty_format
from karadoc.common.utils.assert_utils import assert_true
from karadoc.spark.batch.spark_batch_job import SparkBatchJob

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def load_populate(full_table_name: str) -> SparkBatchJob:
    """Return a non-runnable version of the `job` object declared in the POPULATE.py file of the given table
    with its default variables.

    This method is useful to load any POPULATE for inspecting its metadata (input, output, output_partitions, etc.)
    even when no variable is defined, by using its default values. The `run` method is removed to prevent any misuse
    where one would call the POPULATE without setting the variables properly.

    If you want to execute the `run` method use the `load_runnable_populate` method instead.

    :param full_table_name: Full name of the table (schema_name.table_name)
    :return: the SparkBatchJob object defined in the POPULATE.py file
    """
    return load_non_runnable_action_file(full_table_name, SparkBatchJob)


def load_runnable_populate(full_table_name: str, passed_vars: Optional[Dict[str, str]]) -> SparkBatchJob:
    """Return the `job` object declared in the POPULATE.py file of the given table, override its variables
    and check the load variables match those declared in the POPULATE.py file.

    :param full_table_name: Full name of the table (schema_name.table_name)
    :param passed_vars: Set the variables of the job and check that they match the ones declared
    :return: the SparkBatchJob object defined in the POPULATE.py file
    """
    return load_runnable_action_file(full_table_name, SparkBatchJob, passed_vars)


def print_job_properties(job: SparkBatchJob):
    if job.inputs:
        print(f"inputs : {job.inputs}")
        print(f"input_paths : {job.list_hdfs_inputs()}")
    if job.external_inputs:
        print("external_inputs :")
        print(pretty_format(job.external_inputs))
    if job.output:
        print(f"output : {job.output}")
        if job.output_partition:
            print(f"output_partition : {job.output_partition}")
        print(f"output_path : {job.hdfs_output()}")
    if job.external_outputs:
        print("external_outputs:")
        print(pretty_format(job.external_outputs))


def write_output(job: SparkBatchJob, df: "DataFrame", no_write: bool = False, no_export: bool = False):
    """Serialize the output of a SparkBatchJob if it exists and is not empty, and if its schema is correctly defined.

    :param no_write:
    """
    from pyspark.sql import DataFrame

    if df is None:
        return

    assert_true(isinstance(df, DataFrame))

    if len(df.columns) > 0:
        if no_write:
            df.count()  # We force the DataFrame evaluation
            print("WARN: outputs have been disabled.")
        if len(job.external_outputs) > 0:
            if no_export:
                print("WARN: exports have been disabled.")
            else:
                job.write_external_outputs(df)
        else:
            job.write_table(df)
    else:
        print("WARN: The job returned a DataFrame with zero columns. The output will not be serialized.")
