from karadoc.common.run import Job

job = Job()

job.primary_key = ("c1", "c2", "c3")


def run():
    df = job.spark.createDataFrame(
        [
            (1, 1, 1, 1),
            (2, 2, 2, None),
            (3, 3, None, None),
            (4, None, 4, 4),
            (None, None, 5, 5),
            (None, None, None, 6),
            (None, None, None, 7),
            (None, None, None, 7),
        ],
        "c1 INT, c2 INT, c3 INT, c4 INT",
    )
    return df
