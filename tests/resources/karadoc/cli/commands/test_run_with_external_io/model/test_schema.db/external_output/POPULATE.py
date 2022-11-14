from karadoc.spark.batch import Job

job = Job()

job.external_outputs = {
    "dest1": {
        "connection": "dummy",
        "test": "1",
    },
    "dest2": {
        "connection": "dummy",
        "test": "2",
    },
}

job.output_format = "parquet"


def run():
    return job.spark.sql("""SELECT "b" as b""")
