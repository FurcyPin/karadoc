from karadoc.common.quality import CheckSeverity, Job, alert

job = Job()


@alert(description="", severity=CheckSeverity.Debug, name="alert")
def alert_1():
    pass


@alert(description="", severity=CheckSeverity.Debug, name="alert")
def alert_2():
    pass
