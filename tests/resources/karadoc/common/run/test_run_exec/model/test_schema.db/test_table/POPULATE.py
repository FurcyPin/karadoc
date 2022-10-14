from karadoc.common.run import Job

job = Job()


def run():
    return job.spark.sql("""SELECT 'this is a test DataFrame' as value""")
