from karadoc.common import Job

job = Job()

job.external_inputs = {"input": {"connection": "missing_connection"}}


def run():
    pass
