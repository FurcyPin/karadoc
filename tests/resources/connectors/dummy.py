import os
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter
from pyspark.storagelevel import StorageLevel

from karadoc.common import conf
from karadoc.common.conf import ConfBox
from karadoc.common.connector import Connector
from karadoc.common.stream_utils import batch_to_stream
from karadoc.common.utils.assert_utils import assert_true


def side_effect(s, url):
    print("This is my url : %s" % url)
    print("Performing a dummy side effect : %s" % s)


class DummyConnector(Connector):
    """This is a dummy implementation of a Connector.
    It doesn't write anything and is only useful for testing purposes."""

    def __init__(self, spark: SparkSession, connection_conf: ConfBox):
        super().__init__(spark, connection_conf)

    def write(self, df: DataFrame, dest: Dict):
        df_count = df.persist(StorageLevel.DISK_ONLY).count()
        print(
            "Performing a dummy export: this export doesn't do anything except for evaluating the DataFrame.\n"
            "The exported DataFrame has {df_count} rows".format(df_count=df_count)
        )
        df.printSchema()
        df.show()

    def read(self, source: Dict):
        print("Performing a dummy external read")
        return self.spark.sql("""SELECT 1 as a""")

    def write_stream(self, df: DataFrame, dest: Dict) -> DataStreamWriter:
        assert_true(dest.get("path") is not None, "path param should be specified in source")
        tmp_path = os.path.join(conf.get_spark_stream_tmp_dir(), dest.get("path"))
        return df.writeStream.format("parquet").option("path", tmp_path)

    def read_stream(self, source: Dict):
        assert_true(source.get("table") is not None, "table param should be specified in source")
        return batch_to_stream(
            df=self.spark.sql(f"SELECT * FROM {source['table']}"),
            mode=source.get("mode"),
            relative_tmp_path=source.get("relative_tmp_path"),
            options=source.get("options"),
        )

    def side_effect(self, s):
        print("Performing a dummy side effect : %s" % s)
