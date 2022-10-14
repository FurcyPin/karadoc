from karadoc.common.run import Job

job = Job()


def run():
    return job.spark.sql("""SELECT 1 as a""").limit(0)
