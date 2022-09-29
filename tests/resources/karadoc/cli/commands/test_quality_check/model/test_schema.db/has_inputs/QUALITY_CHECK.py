from karadoc.common.quality import CheckSeverity, Job, alert

job = Job()

job.inputs = {"input_table": "test_schema.input_table"}


def before_all():
    job.load_inputs_as_views()


@alert(
    description="This is a dummy alert generated using inputs tables",
    severity=CheckSeverity.Debug,
)
def alert_has_inputs():
    return job.spark.sql(""" SELECT * FROM input_table where id==1""")
