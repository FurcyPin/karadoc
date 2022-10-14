from pyspark.sql.types import StringType, StructField, StructType

from karadoc.common.run import Job

job = Job()

job.inputs = {"partition_table": "test_schema.partition_table"}

job.output_partition = ["day"]


def run():
    job.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "STATIC")
    # read_table with schema calls spark session getOrCreate
    # and it reset the spark conf options using the builder spark options
    df = job.read_table("partition_table", schema=StructType([StructField("day", StringType())]))
    return df.withColumn("day_value", df["day"])
