from pyspark.sql import DataFrame


def read_spark_table(table_full_name: str) -> DataFrame:
    """Read a spark table

    :param table_full_name:
    :return: a Spark DataFrame
    """
    from karadoc.common.run import Job

    job = Job()
    job.init()
    return job._read_input_table(table_full_name)
