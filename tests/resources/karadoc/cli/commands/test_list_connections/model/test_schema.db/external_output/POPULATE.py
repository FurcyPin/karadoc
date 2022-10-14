from karadoc.common.run import Job

job = Job()

job.external_outputs = {
    "dest1": {
        "connection": "dummy",
        "test": "1",
    },
    "dest2": {
        "connection": "dummy",
        "test": "2",
    },
}


def run():
    pass
