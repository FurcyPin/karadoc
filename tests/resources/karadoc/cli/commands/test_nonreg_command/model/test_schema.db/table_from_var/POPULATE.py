from karadoc.common.run import Job

job = Job()

job.vars = {"var": "a"}


def run():
    var = job.vars["var"]
    return job.spark.createDataFrame([(1, var)], "id INT, var STRING")
