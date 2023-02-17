from karadoc.spark.stream import Job

job = Job()

job.external_output = {"connection": "dummy", "test": "1"}


def stream():
    pass
