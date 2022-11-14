from karadoc.spark.analyze import Job

job = Job()

job.reference_time_col = "cohort"
job.cohorts = ["cohort"]
job.nb_buckets = 5


def analyze():
    job.init()
    spark = job.spark
    df = spark.createDataFrame(
        [
            ("2017-07-24 00:00:00", "DOGS", 1, None),
            ("2017-07-24 00:00:00", "DOGS", 1, None),
            ("2017-07-24 00:00:00", "CATS", 2, None),
            ("2017-07-24 00:00:00", "BIRDS", 2, None),
            ("2017-07-24 00:00:00", None, 3, None),
            ("2017-07-31 00:00:00", "CATS", 4, None),
            ("2017-07-31 00:00:00", "CATS", 5, None),
            ("2017-07-31 00:00:00", "CATS", 6, None),
        ],
        "cohort STRING, col_string STRING, col_int INT, col_null STRING",
    )
    return df
