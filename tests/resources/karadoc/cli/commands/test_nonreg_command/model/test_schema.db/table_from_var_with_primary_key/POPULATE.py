from karadoc.spark.batch import Job

job = Job()

job.vars = {"var": "a"}

job.primary_key = "id"


def run():
    var = job.vars["var"]
    return job.spark.createDataFrame([(1, var)], "id INT, var STRING")
