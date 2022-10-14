from karadoc.common.run import Job

job = Job()

job.vars = {"day": "2018-01-01"}

job.output_partition = [("day", job.vars["day"])]


def run():
    day = job.vars["day"]
    return job.spark.createDataFrame([(1, day), (2, day), (3, day)], "value INT, day_var STRING")
