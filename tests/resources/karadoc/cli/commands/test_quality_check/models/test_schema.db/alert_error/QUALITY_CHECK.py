from karadoc.spark.quality import CheckSeverity, Job, alert

job = Job()


@alert(description="This is a dummy error alert", severity=CheckSeverity.Debug)
def alert_error():
    raise Exception("""This is a test Exception""")
