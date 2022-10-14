from karadoc.common.run import Job

job = Job()

job.vars = {"day": "2018-01-01"}

job.output_partition = [("day", job.vars["day"])]


def run():
    return job.spark.createDataFrame([1, 2, 3], "int")
