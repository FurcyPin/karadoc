from karadoc.common.quality import CheckSeverity, Job, alert, metric

job = Job()


# To avoid duplicate code when you need to define multiple alerts that looks like this:
@alert(
    description="description of test_alert_1",
    severity=CheckSeverity.Debug,
)
def test_alert_1():
    return job.spark.sql(""" SELECT * FROM VALUES (0) WHERE col1 == 1 """)


# You can instead use a generic template that create alerts, like this
def make_alert(i: int):
    @alert(description="description of test_alert_%s" % i, severity=CheckSeverity.Debug, name="test_alert_%s" % i)
    def func():
        return job.spark.sql(f""" SELECT * FROM VALUES (0) WHERE col1 == {i} """)

    job.add_alert(func)


# And then call it like this
make_alert(2)
make_alert(3)


# To avoid duplicate code when you need to define multiple metrics that looks like this:
@metric(
    description="description of test_metric_1",
    severity=CheckSeverity.Debug,
)
def test_metric_1():
    return job.spark.sql(""" SELECT 1 as col1 """)


# You can instead use a generic template that create metrics, like this
def make_metric(i: int):
    @metric(description="description of test_metric_%s" % i, severity=CheckSeverity.Debug, name="test_metric_%s" % i)
    def func():
        return job.spark.sql(f""" SELECT {i} as col1 """)

    job.add_metric(func)


# You can instead use a generic template that create metrics, like this
make_metric(2)
make_metric(3)
