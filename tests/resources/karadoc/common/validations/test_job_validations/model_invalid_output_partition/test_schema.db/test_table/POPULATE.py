from karadoc.spark.batch import Job

job = Job()

job.output_partition = [("day"), ("hour", "01")]


def run():
    pass
