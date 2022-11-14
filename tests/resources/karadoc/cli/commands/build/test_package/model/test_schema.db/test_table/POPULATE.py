from karadoc.spark.batch import Job

job = Job()

from model_lib.functions import var


def run():
    return job.spark.sql(f"select '{var}' as var")
