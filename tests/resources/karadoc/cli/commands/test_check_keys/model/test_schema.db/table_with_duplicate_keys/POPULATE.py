from karadoc.spark.batch import Job

job = Job()

job.primary_key = "c1"
job.secondary_keys = ["c2", "c3"]


def run():
    df = job.spark.createDataFrame(
        [
            (1, 1, 1),
            (2, 2, 2),
            (1, 3, 2),
        ],
        "c1 INT, c2 INT, c3 INT",
    )
    return df
