from karadoc.common.run import Job

job = Job()

job.vars = {"day": "2018-01-01"}

job.output_partition = [("day", job.vars["day"]), "BU", "test"]


def run():
    day = job.vars["day"]
    data = [
        {"day": day, "test": 1, "BU": "FR", "dummie": "dummie1"},
        {"day": day, "test": 2, "BU": "FR", "dummie": "dummie2"},
        {"day": day, "test": 2, "BU": "US", "dummie": "dummie3"},
    ]

    return job.spark.createDataFrame(data)
