import threading
from typing import TYPE_CHECKING, Optional

from karadoc.common.exceptions import IllegalJobInitError
from karadoc.spark.utils import get_spark_session

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _partition_to_path(partition) -> str:
    static_partitions = []
    if partition:
        for element in partition:
            if element and type(element) == tuple:
                static_partitions.append(element)

    if len(static_partitions) > 0:
        return "/" + "/".join(["%s=%s" % (p_key, p_value) for (p_key, p_value) in static_partitions])
    else:
        return ""


def _table_name_to_hdfs_path(base_path, schema_name: str, table_name: str, partition) -> str:
    partition_path = _partition_to_path(partition)
    return "%s/%s.db/%s%s" % (base_path, schema_name, table_name, partition_path)


class HasSpark:
    """Before loading a POPULATE, this global variable must be set to True to prevent users
    from accidentally using the SparkContext outside the `run` method.
    """

    _global_init_lock = False

    def __init__(self) -> None:
        # Attributes that the user is not supposed to change
        self.__spark: Optional["SparkSession"] = None
        self.__threadlocal_spark = threading.local()

    def init(self, app_name: str = None, spark_conf: dict = None):
        if HasSpark._global_init_lock:
            raise IllegalJobInitError("Error: the spark context should not be initialized in an action file.")
        self.__spark = get_spark_session(app_name, spark_conf)

    def detach_spark_session(self) -> None:
        """Replace the SparkSession of this job with a new separate SparkSession that has separate SQLConf,
        registered temporary views and UDFs, but shared SparkContext and table cache.
        :return:
        """
        self.__threadlocal_spark.val = self.__spark.newSession()

    def reset_spark_session(self) -> None:
        """"""
        self.spark.sql("RESET")

    def reset(self) -> None:
        self.__init__()

    @property
    def spark(self) -> Optional["SparkSession"]:
        if hasattr(self.__threadlocal_spark, "val"):
            return self.__threadlocal_spark.val
        else:
            return self.__spark
