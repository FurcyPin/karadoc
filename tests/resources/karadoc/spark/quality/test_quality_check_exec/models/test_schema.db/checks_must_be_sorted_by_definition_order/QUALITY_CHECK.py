from karadoc.spark.quality import CheckSeverity, Job, alert, metric

job = Job()


@alert(
    description="",
    severity=CheckSeverity.Debug,
)
def alert_B():
    pass


@alert(description="", severity=CheckSeverity.Debug)
def alert_A():
    pass


@alert(description="", severity=CheckSeverity.Debug)
def alert_C():
    pass


@metric(
    description="",
    severity=CheckSeverity.Debug,
)
def metric_B():
    pass


@metric(description="", severity=CheckSeverity.Debug)
def metric_A():
    pass


@metric(description="", severity=CheckSeverity.Debug)
def metric_C():
    pass
