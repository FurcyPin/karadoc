from karadoc.spark.batch import Job

job = Job()

job.inputs = {"source": "test_schema.input_table"}


def run():
    job.load_inputs_as_views()
    return job.spark.sql("""SELECT * FROM source""")
