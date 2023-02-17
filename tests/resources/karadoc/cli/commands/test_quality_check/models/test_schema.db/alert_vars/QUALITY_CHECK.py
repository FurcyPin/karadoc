from karadoc.spark.quality import CheckSeverity, Job, alert

job = Job()

job.vars = {"day": "2020-01-01"}


@alert(description="This is a ko alert using vars", severity=CheckSeverity.Debug)
def alert_ko():
    return job.spark.createDataFrame(
        [
            (1234, job.vars["day"]),
        ],
        "id INT, `day` STRING",
    )
