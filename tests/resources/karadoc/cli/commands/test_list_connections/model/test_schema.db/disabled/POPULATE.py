from karadoc.spark.batch import Job

job = Job()

job.external_inputs = {"source": {"connection": "dummy_disabled"}}

job.disable = True

job.output_format = "parquet"


def run():
    pass
