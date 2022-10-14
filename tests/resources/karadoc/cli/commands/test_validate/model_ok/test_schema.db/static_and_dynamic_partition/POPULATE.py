from karadoc.common.run import Job

job = Job()

job.vars = {"day": "2018-01-01"}

job.output_partition = [("day", job.vars["day"]), "BU", "test"]


def run():
    data = [
        {"day": "2018-01-01", "test": 1, "BU": "FR", "dummie": "dummie1"},
        {"day": "2018-01-01", "test": 2, "BU": "FR", "dummie": "dummie2"},
        {"day": "2018-01-01", "test": 2, "BU": "US", "dummie": "dummie3"},
    ]

    return job.spark.createDataFrame(data)
