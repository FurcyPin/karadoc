from karadoc.common.stream import Job
from karadoc.common.stream_utils import batch_to_stream

job = Job()

job.vars = {"day": "2018-01-01"}

job.output_partition = [("day", job.vars["day"])]


def stream():
    day = job.vars["day"]
    df = job.spark.createDataFrame([(1, day), (2, day), (3, day)], "value INT, day STRING")
    return batch_to_stream(df)
