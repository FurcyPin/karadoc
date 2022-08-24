from typing import Dict

from pyspark import Row
from pyspark.sql import DataFrame

from karadoc.common import Job
from karadoc.common.utils.assert_utils import assert_true

job = Job()

job.external_inputs = {"source": {"connection": "dummy"}}

job.external_outputs = {"dest": {"connection": "dummy"}}


def run():
    return job.read_external_input("source")


def read_external_input(source: Dict) -> DataFrame:
    rows = [Row(id=i) for i in range(2)]
    return job.spark.createDataFrame(rows)


def write_external_output(df: DataFrame, dest: Dict) -> None:
    assert_true(dest == {"connection": "dummy"})
    job.write_external_output_called = True
