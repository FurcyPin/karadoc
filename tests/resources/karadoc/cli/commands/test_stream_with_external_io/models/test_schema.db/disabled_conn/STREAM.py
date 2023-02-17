from karadoc.spark.stream import Job

job = Job()

job.external_inputs = {"source": {"connection": "dummy"}}


def stream():
    pass
