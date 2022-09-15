from karadoc.common.stream import Job
from karadoc.common.stream_utils import batch_to_stream

job = Job()

job.output_format = "json"


def stream():
    return batch_to_stream(job.spark.sql("select 'a' as a"))
