from karadoc.common.run import Job

job = Job()

job.primary_key = "`id.id`"
job.secondary_keys = [("json1.a", "json2.c"), "json2.d"]


def run():
    df = job.spark.createDataFrame(
        [
            (1, {"a": None, "b": 1}, {"c": 1, "d": 1}),
            (2, {"a": 3, "b": 2}, {"c": 2, "d": 2}),
            (3, {"a": 3, "b": 3}, {"c": 3, "d": 3}),
        ],
        "`id.id` INT, json1 STRUCT<a: BIGINT, b: BIGINT>, json2 STRUCT<c: BIGINT, d:BIGINT>",
    )
    return df
