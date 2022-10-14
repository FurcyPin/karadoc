from karadoc.common.run import Job

job = Job()

job.disable = True

job.inputs = {
    "A2": "alive.A2",
    "D2": "dead.D2",
}


def run():
    pass
