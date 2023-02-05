from karadoc.spark.stream import Job
from karadoc.spark.stream_utils import batch_to_stream

job = Job()

job.vars = {"day": "2018-01-01"}

job.output_partition = [("day", job.vars["day"]), "BU", "test"]


def stream():
    day = job.vars["day"]
    data = [
        {"day": day, "test": 1, "BU": "FR", "dummie": "dummie1"},
        {"day": day, "test": 2, "BU": "FR", "dummie": "dummie2"},
        {"day": day, "test": 2, "BU": "US", "dummie": "dummie3"},
    ]

    return batch_to_stream(job.spark.createDataFrame(data))
