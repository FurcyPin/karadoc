from karadoc.common.stream_utils import batch_to_stream
from karadoc.spark.stream import Job

job = Job()

job.vars = {"day": "2018-01-01"}

job.output_partition = ["BU", ("day", job.vars["day"])]


def stream():
    data = [{"day": "2018-01-01", "test": 1, "BU": "FR"}, {"day": "2018-01-01", "test": 3, "BU": "US"}]

    return batch_to_stream(job.spark.createDataFrame(data))
