from karadoc.spark.batch import Job

job = Job()

job.vars = {"day": "2018-01-01"}

job.output_partition = ["BU", ("day", job.vars["day"])]


def run():
    data = [{"day": "2018-01-01", "test": 1, "BU": "FR"}, {"day": "2018-01-01", "test": 3, "BU": "US"}]

    return job.spark.createDataFrame(data)
