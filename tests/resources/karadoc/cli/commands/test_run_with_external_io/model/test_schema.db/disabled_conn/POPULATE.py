from karadoc.spark.batch import Job

job = Job()

job.external_inputs = {"source": {"connection": "dummy"}}

job.output_format = "parquet"


def run():
    return job.read_external_inputs()["source"]
