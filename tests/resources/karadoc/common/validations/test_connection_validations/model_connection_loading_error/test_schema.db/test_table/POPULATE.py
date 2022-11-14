from karadoc.spark.batch import Job

job = Job()

job.external_inputs = {"input": {"connection": "test_conn"}}


def run():
    pass
