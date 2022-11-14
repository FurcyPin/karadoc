from karadoc.spark.batch import Job

job = Job()

job.external_inputs = {"input": {"connection": "missing_connection"}}

job.disable = True


def run():
    pass
