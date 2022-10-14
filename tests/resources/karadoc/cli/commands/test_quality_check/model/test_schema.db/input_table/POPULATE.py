from pyspark.sql.types import Row

from karadoc.common.run import Job

job = Job()


def run():
    rows = [Row(id=i) for i in range(2)]
    return job.spark.createDataFrame(rows)
