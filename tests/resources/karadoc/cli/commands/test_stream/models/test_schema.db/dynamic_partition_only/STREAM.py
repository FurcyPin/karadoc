from karadoc.spark.stream import Job
from karadoc.spark.stream_utils import batch_to_stream

job = Job()

job.output_partition = ["day", "BU"]


def stream():
    data = [
        {"day": "2018-01-01", "test": 1, "BU": "FR"},
        {"day": "2018-01-02", "test": 2, "BU": "FR"},
        {"day": "2018-01-01", "test": 3, "BU": "US"},
        {"day": "2018-01-02", "test": 4, "BU": "US"},
        {"day": "2018-01-03", "test": 5, "BU": "US"},
    ]

    return batch_to_stream(job.spark.createDataFrame(data))
