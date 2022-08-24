from pyspark.sql import SparkSession

from karadoc.common.conf import ConfBox
from karadoc.common.connector import Connector


class ConnectorMock(Connector):
    def __init__(self, spark: SparkSession, connection_conf: ConfBox):
        super().__init__(spark, connection_conf)

    def read(self, source):
        return self.spark.sql("select 'a' as a")
