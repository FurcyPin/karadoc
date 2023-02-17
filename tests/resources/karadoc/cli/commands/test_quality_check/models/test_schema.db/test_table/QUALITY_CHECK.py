from karadoc.spark.quality import CheckSeverity, Job, alert, metric

job = Job()


@alert(
    description="This is a dummy ok alert",
    severity=CheckSeverity.Debug,
)
def test_alert_ok():
    return job.spark.sql(""" SELECT * FROM VALUES (1) WHERE col1 == 0 """)


@alert(description="This is a dummy ko alert", severity=CheckSeverity.Debug)
def test_alert_ko():
    return job.spark.sql(""" SELECT "ABC" as col1, 123 as col2, 456 as col3 """)


@metric(description="This is a dummy metric", severity=CheckSeverity.Debug)
def test_metric():
    return job.spark.sql(""" SELECT 1 as col1 """)
