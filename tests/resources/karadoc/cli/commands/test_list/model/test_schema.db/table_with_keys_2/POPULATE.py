from karadoc.common import Job

job = Job()

job.primary_key = ("c1", "c3")
job.secondary_keys = ["c1", "c2"]


def run():
    pass
