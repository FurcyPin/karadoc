from karadoc.common.stream_utils import batch_to_stream
from karadoc.spark.stream import Job

job = Job()

job.external_output = {"connection": "dummy", "test": "1", "table": "test_external_output"}


def stream():
    return batch_to_stream(job.spark.sql("""SELECT 'b' as b"""))
