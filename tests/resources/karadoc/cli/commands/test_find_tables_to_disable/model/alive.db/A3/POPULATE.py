from karadoc.spark.batch import Job

job = Job()

job.inputs = {
    "A2": "alive.A2",
}

job.external_outputs = {"dest": {"connection": "dummy"}}


def run():
    pass
