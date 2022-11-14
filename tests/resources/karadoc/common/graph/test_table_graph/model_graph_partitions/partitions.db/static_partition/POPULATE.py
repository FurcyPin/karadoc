from karadoc.spark.batch import Job

job = Job()

job.output_partition = [("day", "2020-01-01")]


def run():
    pass
