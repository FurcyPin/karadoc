from karadoc.spark.batch import Job

job = Job()

job.vars = {"day": "2020-01-01"}

job.output_partition = [("day", job.vars["day"])]


def run():
    pass
