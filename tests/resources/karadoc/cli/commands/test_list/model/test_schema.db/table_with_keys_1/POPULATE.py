from karadoc.common.run import Job

job = Job()

job.primary_key = ("c1", "c3")
job.secondary_keys = ["c1", "c2"]

job.disable = True


def run():
    pass
