from pyspark.sql import functions as f
from udfs.dummy_udf import dummy_udf

from karadoc.spark.batch import Job

job = Job()


def run():
    df = job.spark.sql("""SELECT 1 as a""")
    return df.withColumn("a", dummy_udf(f.col("a")))
