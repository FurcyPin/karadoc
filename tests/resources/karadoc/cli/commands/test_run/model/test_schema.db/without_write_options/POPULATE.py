from karadoc.common.run import Job

job = Job()

job.output_format = "json"


def run():
    return job.spark.sql("select 'a' as a")
