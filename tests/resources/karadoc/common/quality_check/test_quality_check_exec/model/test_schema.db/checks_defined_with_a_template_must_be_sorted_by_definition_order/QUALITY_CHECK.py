from karadoc.common.quality import CheckSeverity, Job, alert, metric

job = Job()


# To avoid duplicate code when you need to define multiple alerts that looks like this:
@alert(
    description="description of alert_1",
    severity=CheckSeverity.Debug,
)
def alert_1():
    return job.spark.sql(""" SELECT * FROM VALUES (0) WHERE col1 == 1 """)


# You can instead use a generic template that create alerts, like this
def gen_alert(i: int):
    @alert(description="description of alert_%s" % i, severity=CheckSeverity.Debug, name="alert_%s" % i)
    def func():
        return job.spark.sql(f""" SELECT * FROM VALUES (0) WHERE col1 == {i} """)

    job.add_alert(func)


# And then call it like this
gen_alert(2)
gen_alert(3)


# To avoid duplicate code when you need to define multiple metrics that looks like this:
@metric(
    description="description of metric_1",
    severity=CheckSeverity.Debug,
)
def metric_1():
    return job.spark.sql(""" SELECT 1 as col1 """)


# You can instead use a generic template that create metrics, like this
def gen_metric(i: int):
    @metric(description="description of metric_%s" % i, severity=CheckSeverity.Debug, name="metric_%s" % i)
    def func():
        return job.spark.sql(f""" SELECT {i} as col1 """)

    job.add_metric(func)


# You can instead use a generic template that create metrics, like this
gen_metric(2)
gen_metric(3)
