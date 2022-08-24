from karadoc.common.stream import Job

job = Job()

job.external_inputs = {
    "source": {
        "connection": "dummy",
        "table": "external_input_test_table",
        "mode": "append",
        "relative_tmp_path": "external_input_test_table",
        "options": {"maxFilesPerTrigger": "100000"},
    }
}

job.external_output = {"connection": "dummy", "path": "test_external_output.table"}

job.vars = {"var": "4"}


def stream():
    var = job.vars["var"]
    job.spark.sql(f"select {var} as a").createOrReplaceTempView("external_input_test_table")
    return job.read_external_inputs()["source"]
