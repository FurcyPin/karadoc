from karadoc.common.run import Job

job = Job()

job.external_inputs = {"source": {"connection": "dummy"}}

job.external_outputs = {"dest": {"connection": "dummy"}}


def run():
    return job.read_external_inputs()["source"]
