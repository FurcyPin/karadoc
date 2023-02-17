from karadoc.spark.quality import CheckSeverity, Job, alert

job = Job()


@alert(description="This is a dummy ko alert", severity=CheckSeverity.Debug)
def alert_ko():
    return job.spark.createDataFrame(
        [
            (1234, "this is a dummy error"),
        ],
        "id INT, `error.error` STRING",
    )
