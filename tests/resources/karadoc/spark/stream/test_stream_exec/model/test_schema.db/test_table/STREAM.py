from karadoc.spark.stream import Job
from karadoc.spark.stream_utils import batch_to_stream

job = Job()


def stream():
    return batch_to_stream(job.spark.sql("""SELECT 'this is a test DataFrame' as value"""))
