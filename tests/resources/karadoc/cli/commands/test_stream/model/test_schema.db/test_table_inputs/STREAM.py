from karadoc.common.stream import Job

job = Job()

job.inputs = {"test_table": "test_schema.test_table"}


def stream():
    return job.read_table("test_table")
