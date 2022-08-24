from karadoc.common.stream import Job

job = Job()

job.external_inputs = {"source": {"connection": "dummy", "table": "external_input_test_table"}}


def stream():
    job.spark.sql("select 'a' as a").createOrReplaceTempView("external_input_test_table")
    return job.read_external_inputs()["source"]
