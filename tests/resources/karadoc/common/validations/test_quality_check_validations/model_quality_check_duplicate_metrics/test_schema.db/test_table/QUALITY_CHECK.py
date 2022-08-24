from karadoc.common.quality import CheckSeverity, Job, metric

job = Job()


@metric(description="", severity=CheckSeverity.Debug, name="metric")
def metric_1():
    pass


@metric(description="", severity=CheckSeverity.Debug, name="metric")
def metric_2():
    pass
