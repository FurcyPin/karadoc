from karadoc.spark.batch import Job

job = Job()

job.external_inputs = {"dummy_source": {"connection": "dummy"}}

job.output_format = "parquet"

job.query = "SELECT b as c FROM dummy_source"


def run():
    job.load_external_inputs_as_views()
    return job.spark.sql(job.query)
