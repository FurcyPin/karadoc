from karadoc.common.run import Job

job = Job()

job.inputs = {"source": "test_schema.input_table"}


def run():
    return job.read_table("source")
