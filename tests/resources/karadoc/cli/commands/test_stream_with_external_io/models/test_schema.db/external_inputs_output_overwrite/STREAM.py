from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

from karadoc.spark.stream import Job
from karadoc.spark.stream_utils import batch_to_stream

job = Job()

job.external_inputs = {"source": {"connection": "dummy", "table": "external_input_test_table"}}

job.external_output = {"connection": "dummy"}


def stream():
    job.spark.sql("select 'a' as a").createOrReplaceTempView("external_input_test_table")
    return job.read_external_inputs()["source"]


def read_external_inputs() -> Dict[str, DataFrame]:
    return {"source": batch_to_stream(job.spark.sql("""SELECT 'b' as b"""))}


def write_external_output(df: DataFrame, dest: Dict) -> DataStreamWriter:
    job.write_external_output_called = True
    return df.writeStream.format("memory").queryName("test_stream_with_external_inputs_outputs_overwrite")
