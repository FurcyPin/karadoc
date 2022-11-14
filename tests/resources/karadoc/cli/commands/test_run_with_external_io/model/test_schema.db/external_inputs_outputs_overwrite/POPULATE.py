from typing import Dict

from pyspark import Row
from pyspark.sql import DataFrame

from karadoc.spark.batch import Job

job = Job()

job.external_inputs = {"source": {"connection": "dummy"}}

job.external_outputs = {"dest": {"connection": "dummy"}}


def run():
    return job.read_external_inputs()["dest"]


def read_external_inputs() -> Dict[str, DataFrame]:
    rows = [Row(id=i) for i in range(2)]
    return {"dest": job.spark.createDataFrame(rows)}


def write_external_outputs(df: DataFrame) -> None:
    job.write_external_outputs_called = True
