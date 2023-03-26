from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional, Tuple, Union

from karadoc.common import conf
from karadoc.common.table_utils import parse_table_name
from karadoc.spark.job_core.has_spark import HasSpark, _table_name_to_hdfs_path

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _output_partition_to_dynamic_partitions(partition) -> List[str]:
    dynamic_partitions = []
    if partition:
        for element in partition:
            if element and type(element) == str:
                dynamic_partitions.append(element)
    return dynamic_partitions


class HasOutput(HasSpark, ABC):
    def __init__(self) -> None:
        super().__init__()
        # Attributes that the user may change
        self.output_partition: List[Union[Tuple[str, ...], str]] = []
        self.output_mode = "OVERWRITE"
        self.output_options = {}

        # Attributes that the user is not supposed to change
        self.output: Optional[str] = None
        self.output_warehouse_dir = conf.get_warehouse_folder_location()

    @property
    def output_partition_names(self) -> List[str]:
        """Returns the names of the output partitions.
        Works whether dynamic or static partitioning is used.

        :return: a list of string
        """
        return [p[0] if isinstance(p, Tuple) else p for p in self.output_partition]

    @abstractmethod
    def write_table(self, df: "DataFrame"):
        pass

    def hdfs_output(self) -> str:
        (schema_name, table_name, _) = parse_table_name(self.output)
        return _table_name_to_hdfs_path(self.output_warehouse_dir, schema_name, table_name, self.output_partition)

    def dynamic_partitions(self) -> List[str]:
        return _output_partition_to_dynamic_partitions(self.output_partition)
