from karadoc.spark.quality import CheckSeverity, Job, alert

job = Job()


@alert(
    description="This is a dummy ok alert",
    severity=CheckSeverity.Debug,
)
def alert_ok():
    return job.spark.sql(""" SELECT * FROM VALUES (1) WHERE col1 == 0 """)
