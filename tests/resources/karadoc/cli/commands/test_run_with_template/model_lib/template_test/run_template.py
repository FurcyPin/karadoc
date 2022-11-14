from karadoc.spark.batch import Job

job = Job()


def run():
    return job.spark.sql("select 'test_value' as test_col")
