from karadoc.common.stream import Job
from karadoc.common.stream_utils import batch_to_stream

job = Job()

job.disable = True


def stream():
    return batch_to_stream(job.spark.sql("""SELECT 'this is a test DataFrame' as value"""))
