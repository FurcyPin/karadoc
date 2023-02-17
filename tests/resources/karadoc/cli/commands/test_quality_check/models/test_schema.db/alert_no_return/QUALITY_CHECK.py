from karadoc.spark.quality import CheckSeverity, Job, alert

job = Job()


@alert(description="This is a dummy incorrect alert that returns nothing", severity=CheckSeverity.Debug)
def alert_no_return():
    return None
