from karadoc.spark.quality import CheckSeverity, Job, alert, metric

job = Job()


@alert(
    description="description of test_alert_1",
    severity=CheckSeverity.Debug,
)
def test_alert_1():
    return job.spark.sql(""" SELECT * FROM VALUES (1) WHERE col1 == 0 """)


@alert(description="description of test_alert_2", severity=CheckSeverity.Debug)
def test_alert_2():
    return job.spark.sql(""" SELECT * FROM VALUES (1) WHERE col1 == 0 """)


@metric(description="description of test_metric_1", severity=CheckSeverity.Debug)
def test_metric_1():
    return job.spark.sql(""" SELECT 1 as col1 """)


@metric(description="description of test_metric_2", severity=CheckSeverity.Debug)
def test_metric_2():
    return job.spark.sql(""" SELECT 1 as col1 """)
