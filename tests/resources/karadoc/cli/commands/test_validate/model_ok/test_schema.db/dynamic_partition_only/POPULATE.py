from karadoc.common.run import Job

job = Job()

job.output_partition = ["day", "BU"]


def run():
    data = [
        {"day": "2018-01-01", "test": 1, "BU": "FR"},
        {"day": "2018-01-02", "test": 2, "BU": "FR"},
        {"day": "2018-01-01", "test": 3, "BU": "US"},
        {"day": "2018-01-02", "test": 4, "BU": "US"},
        {"day": "2018-01-03", "test": 5, "BU": "US"},
    ]

    return job.spark.createDataFrame(data)
