from karadoc.common import Job

job = Job()

job.external_outputs = {"input": {"connection": "test_conn"}}

job.disable = True


def run():
    pass
