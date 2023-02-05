import os
import uuid
from typing import Dict, Optional

from pyspark.sql import DataFrame

from karadoc.common import conf
from karadoc.common.utils.assert_utils import assert_true


def stream_to_batch(stream_df: DataFrame) -> DataFrame:
    """Transform a streaming dataframe to a batch dataframe.

    :param stream_df: A Spark streaming dataframe
    :return: batch dataframe
    """
    assert_true(stream_df.isStreaming, "dataframe parameter must be a streaming dataframe")
    random_name = "df_" + str(uuid.uuid4()).replace("-", "_")
    stream_df.writeStream.format("memory").queryName(f"df_{random_name}").trigger(once=True).start().awaitTermination()
    df = stream_df.sql_ctx.sparkSession.sql(f"""SELECT * FROM df_{random_name}""")  # nosec B608
    return df


def batch_to_stream(
    df: DataFrame,
    mode: str = "OVERWRITE",
    relative_tmp_path: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """Transform a batch dataframe to a streaming dataframe.

    :param df: A Spark dataframe
    :param mode: Specifies the behavior when data or table already exists.
        * `append`: Append stream_df dataframe to existing data in the relative_tmp_path.
        * `overwrite`: Overwrite existing data.
        * `error` or `errorifexists`: Throw an exception if data already exists.
        * `ignore`: Silently ignore this operation if data already exists.

    :param relative_tmp_path: A relative tmp path will be used to store temporarily the spark df
    :param options: Specifies the behavior when data or table already exists.
    :return: streaming dataframe
    """
    if options is None:
        options = {}
    assert_true(not df.isStreaming, "dataframe parameter must be a batch dataframe")
    if relative_tmp_path is None:
        relative_tmp_path = str(uuid.uuid4()).replace("-", "_")
    relative_tmp_path = os.path.join(conf.get_spark_stream_tmp_dir(), relative_tmp_path)
    df.write.mode(mode).parquet(relative_tmp_path)
    ds = df.sql_ctx.sparkSession.readStream.options(**options).schema(df.schema).parquet(relative_tmp_path)
    return ds
