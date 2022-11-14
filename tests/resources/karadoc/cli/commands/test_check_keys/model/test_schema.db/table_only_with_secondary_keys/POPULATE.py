from karadoc.spark.batch import Job

job = Job()

job.secondary_keys = [("c1", "c2"), "c3"]


def run():
    df = job.spark.createDataFrame(
        [(1, 1, "a", "b", "c"), (2, 2, "a", None, None), (3, None, "a", "b", "c")],
        "c1 INT, c2 INT, c3 STRING, c4 STRING, c5 STRING",
    )
    return df
