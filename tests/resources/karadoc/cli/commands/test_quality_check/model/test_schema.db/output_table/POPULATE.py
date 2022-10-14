from karadoc.common.run import Job

job = Job()

job.external_outputs = {"dest": {"connection": "dummy"}}


def run():
    return job.spark.sql("""SELECT "b" as b""")
