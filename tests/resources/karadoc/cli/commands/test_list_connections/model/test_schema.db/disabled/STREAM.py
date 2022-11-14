from karadoc.spark.stream import Job

job = Job()

job.external_inputs = {"source": {"connection": "dummy_disabled"}}

job.disable = True


def stream():
    pass
