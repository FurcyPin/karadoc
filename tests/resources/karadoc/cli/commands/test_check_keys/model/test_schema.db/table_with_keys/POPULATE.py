from karadoc.common.run import Job

job = Job()

job.primary_key = ("c1", "c3")
job.secondary_keys = ["c1", "c2"]


def run():
    df = job.spark.createDataFrame(
        [(1, 1, "a", "b", "c"), (2, 2, "a", None, None), (3, None, "a", "b", "c")],
        "c1 INT, c2 INT, c3 STRING, c4 STRING, c5 STRING",
    )
    return df
