from typing import List, Optional, Tuple, Union

from pyspark.sql import DataFrame

from karadoc.common import conf
from karadoc.common.job_core.has_spark import HasSpark, _table_name_to_hdfs_path
from karadoc.common.table_utils import parse_table_name
from karadoc.spark.utils import write_table


def _output_partition_to_dynamic_partitions(partition) -> List[str]:
    dynamic_partitions = []
    if partition:
        for element in partition:
            if element and type(element) == str:
                dynamic_partitions.append(element)
    return dynamic_partitions


class HasOutput(HasSpark):
    def __init__(self) -> None:
        # Attributes that the user may change
        self.output_partition: List[Union[Tuple[str, ...], str]] = []
        self.output_format = "parquet"
        self.output_mode = "OVERWRITE"
        self.output_options = {}

        # Attributes that the user is not supposed to change
        self.output: Optional[str] = None
        self.output_warehouse_dir = conf.get_warehouse_folder_location()

    @property
    def output_partitioning_type(self) -> Optional[str]:
        """Indicates the type of partitioning of the output.
        Possible return values are:
        - None: no partitioning
        - "static": dynamic partitioning
        - "dynamic": static partitioning

        :return: either None, "static", or "dynamic"
        """
        if self.output_partition:
            if type(self.output_partition[0]) == str:
                return "dynamic"
            else:
                return "static"
        else:
            return None

    @property
    def output_partition_names(self) -> List[str]:
        """Returns the names of the output partitions.
        Works whether dynamic or static partitioning is used.

        :return: a list of string
        """
        output_partitioning_type = self.output_partitioning_type
        if output_partitioning_type == "dynamic":
            return [p_name for p_name in self.output_partition]
        elif output_partitioning_type == "static":
            return [p_name for p_name, p_value in self.output_partition]
        else:
            return []

    def write_table(self, df: DataFrame):
        write_table(
            df=df,
            path=self.hdfs_output(),
            output_format=self.output_format,
            output_mode=self.output_mode,
            output_options=self.output_options,
            partitions=self.dynamic_partitions(),
        )

    def hdfs_output(self) -> str:
        (schema_name, table_name, _) = parse_table_name(self.output)
        return _table_name_to_hdfs_path(self.output_warehouse_dir, schema_name, table_name, self.output_partition)

    def dynamic_partitions(self) -> List[str]:
        return _output_partition_to_dynamic_partitions(self.output_partition)
