from typing import Optional

from pyspark.sql import DataFrame, SparkSession


def get_spark_session(app_name: Optional[str] = None, extra_spark_conf: Optional[dict] = None) -> SparkSession:
    """Gets an existing :class:`SparkSession` or, if there is no existing one, creates a new one.

    Spark default settings can be set via the `{env}.spark.conf` in `settings.toml`, and can also be overridden
    using this method's optional argument `extra_spark_conf`.

    Example:

    In your settings.toml:

    .. code-block:: python

        # In settings.toml, this setting configures the default driver memory on all environments
        [default.spark.conf]
          spark.driver.memory="4g"

        # While this setting overrides the default driver memory in the "prod" environment
        [prod.spark.conf]
          spark.driver.memory="6g"

    # It can also be overridden via the command line for a specific job like this :
    karadoc run --spark-conf spark.driver.memory=8g ...

    :param app_name:
    :param extra_spark_conf: Spark configuration parameters to over
    :return:
    """
    # TODO: this late import is a temporary workaround for cyclic dependencies
    from karadoc.common import conf
    from karadoc.spark.conf import get_spark_conf

    if extra_spark_conf is None:
        extra_spark_conf = {}
    spark_conf = {**get_spark_conf(), **extra_spark_conf}
    builder: SparkSession.Builder = SparkSession.builder
    if conf.is_dev_env():
        builder = builder.master("local[4]")
    for k, v in spark_conf.items():
        builder = builder.config(k, v)
    if app_name is None:
        app_name = conf.APPLICATION_NAME.lower()
    spark: SparkSession = builder.appName(app_name).getOrCreate()

    # As a workaround to this spark issue https://issues.apache.org/jira/browse/SPARK-38870,
    # we reset the builder options
    spark.Builder._options = {}
    return spark


def write_table(df: DataFrame, path: str, output_format: str, output_mode: str, output_options=None, partitions=None):
    # TODO: this late import is a temporary workaround for cyclic dependencies
    from karadoc.common import conf

    if output_options is None:
        output_options = {}
    if partitions is None:
        partitions = []
    writer = df.write.partitionBy(partitions).mode(output_mode).format(output_format)
    write_options = conf.get_write_options_for_format(output_format)
    options = {**write_options, **output_options}
    writer.options(**options).save(path)
