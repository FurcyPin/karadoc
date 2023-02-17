from typing import TYPE_CHECKING

from karadoc.spark.conf import get_batch_default_output_format, get_write_options_for_format
from karadoc.spark.job_core.has_output import HasOutput

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class HasBatchOutput(HasOutput):
    def __init__(self) -> None:
        super().__init__()
        self.output_format = get_batch_default_output_format()
        self.output_mode = "OVERWRITE"

    def write_table(self, df: "DataFrame"):
        self._write_table(
            df=df,
            path=self.hdfs_output(),
            output_format=self.output_format,
            output_mode=self.output_mode,
            output_options=self.output_options,
            partitions=self.dynamic_partitions(),
        )

    @staticmethod
    def _write_table(
        df: "DataFrame", path: str, output_format: str, output_mode: str, output_options=None, partitions=None
    ):
        if output_options is None:
            output_options = {}
        if partitions is None:
            partitions = []
        writer = df.write.partitionBy(partitions).mode(output_mode).format(output_format)
        write_options = get_write_options_for_format(output_format)
        options = {**write_options, **output_options}
        writer.options(**options).save(path)
