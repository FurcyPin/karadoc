from pyspark.sql import DataFrame


def read_spark_table(table_full_name: str) -> DataFrame:
    """Read a spark table

    :param table_full_name:
    :return: a Spark DataFrame
    """
    from karadoc.common.run.spark_batch_job import SparkBatchJob

    job = SparkBatchJob()
    job.init()
    return job._read_input_table(table_full_name)
