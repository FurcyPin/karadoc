import importlib
from abc import ABC
from typing import TYPE_CHECKING, Any, Dict, Optional

from karadoc.common.class_utils import find_class_from_module
from karadoc.common.conf import ConfBox, get_connection_conf
from karadoc.common.conf.configurable_class import ConfigurableClass, ConfParam

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class SparkConnector(ABC):
    """Users can implement this class to write their own custom connectors.

    Implementations may override the following methods:

    - `read(self, source: Dict) -> DataFrame` : reads from an external source and return a Spark DataFrame (batch)
    - `write(self, df: DataFrame, dest: Dict)` : writes a given DataFrame to an external destination (batch)
    - `read_stream(self, source: Dict) -> DataFrame` : reads from an external streaming source and
      return a Spark DataFrame
    - `write_stream(self, df: DataFrame, dest: Dict)`  : writes a given streaming DataFrame to an external sink

    They can also have their own public methods for side-effects.
    If you want to call a connector's method with side-effects, it is recommended to NOT do it in the ACTION_FILE's
    `run`/`stream` method, but instead by overriding on of the following methods:

    - `read_external_input(source: Dict) -> DataFrame` : read a single external input and return a DataFrame
    - `write_external_output(df: DataFrame, dest: Dict) -> DataFrame` : write a DataFrame to a single external output

    This will make regression-testing easier.

    In some very specific cases, one might want to read from multiple sources or write to multiple destinations
    by having shared side-effects between them (for instance, if you want to read two DataFrames from a single
    source with a very heavy initialization cost, or if you want to write to two destination and rollback the first
    if the second fails). In such case, you might want to overwrite the following methods instead:

    - `read_external_inputs() -> Dict[str, DataFrame]` : read all external inputs declared in this job and
      return a Dict(alias, DataFrame)
    - `write_external_outputs(df: DataFrame): Write a DataFrame to all external outputs declared in
      this job (not available in streaming)
    """

    spark: "SparkSession" = None
    conf: "ConfBox" = None

    def __init__(self, spark: "SparkSession", conf: ConfBox) -> None:
        """Instantiates the `Connector` base abstract class

        :param spark: The Spark Session
        :param conf: A DynaBox or a dict-like object
        """
        self.conf = conf
        self.spark = spark

    def write(self, df: "DataFrame", dest: Dict[str, Any]):
        """Write the given DataFrame to an external output described by `dest`

        :param df: the DataFrame to export
        :param dest: A dictionary describing the destination
        :return:
        """
        raise NotImplementedError()

    def write_stream(self, df: "DataFrame", dest: Dict[str, Any]):
        """Write the given streaming DataFrame to an external output described by `dest`

        :param df:
        :param dest:
        :return:
        """
        raise NotImplementedError()

    def read(self, source: Dict[str, Any]) -> "DataFrame":
        """Write the given external input described by `source` and return it as a DataFrame

        :param source:
        :return:
        """
        raise NotImplementedError()

    def read_stream(self, source: Dict[str, Any]) -> "DataFrame":
        """Write the given external input described by `source` and return it as a streaming DataFrame

        :param source:
        :return:
        """
        raise NotImplementedError()

    def test_connection(self):
        """Test that the connection works without reading or writing anything
        (or by writing a test output and deleting it afterwards, depending on the implementation)"""
        return NotImplemented


class ConfigurableSparkConnector(ConfigurableClass, SparkConnector, ABC):
    """Improved version of the :class:`Connector` class, that also extends :class:`ConfigurableClass`.

    Connectors inheriting from this class will benefit from automatic parameter validation.
    """

    type = ConfParam(type=str, required=True, description="Type of the connector")

    disable = ConfParam(
        type=bool,
        required=False,
        default=False,
        description="Set to true to disable this connector. Only disabled jobs may reference disabled connectors.",
    )

    def __init__(self, spark: "SparkSession", conf: ConfBox):
        ConfigurableClass.__init__(self, conf)
        SparkConnector.__init__(self, spark, conf)


def load_connector(connection_name: str, spark: "SparkSession") -> SparkConnector:
    """Load a connector configured with the given connection.

    This method will look in the settings.toml for an entry that looks like this:

    [{current_env}.connection.{connection_name}]
      type = {module_name}
      {connection_setting_key_1} = {connection_setting_value_1}
      ...

    It will then load the specified module which should define a class that inherits from
    :class:`karadoc.common.connector.Connector` and build a Connector of that type,
    configured with the {connection_settings} found in the settings.toml

    :param connection_name: Name of a connection defined in settings.toml
    :param spark: a Spark Session
    :return:
    """
    return _load_connector_for_env(connection_name, spark, None)


def _load_connector_for_env(connection_name: str, spark: "SparkSession", env: Optional[str]) -> SparkConnector:
    """Load a connector for the specified connection name in the specified dynaconf environment.

    This method will look in the settings.toml for an entry that looks like this:

    [{env}.connection.{connection_name}]
      type = {module_name}
      {connection_setting_key_1} = {connection_setting_value_1}
      ...

    It will then load the specified module which should define a class that inherits from
    :class:`karadoc.common.connector.Connector` and build a Connector of that type,
    configured with the {connection_settings} found in the settings.toml

    Warning: If two implementations of Connector are defined in the same module, karadoc will not be able to know which
    one to pick. Therefore each implementation of Connector must be kept in a separate module.

    :param connection_name: Name of a connection defined in settings.toml
    :param spark: a Spark Session
    :param env: The environment on which to load the connection
        (useful for running validations on multiple environments)
    :return:
    """
    connection_conf = get_connection_conf(connection_name, env)
    connector_mock = connection_conf.get("mock")
    if connector_mock is not None:
        module_name = connector_mock
    else:
        module_name = connection_conf["type"]
    module = importlib.import_module(module_name)
    connector_class = find_class_from_module(module, SparkConnector)
    return connector_class(spark, connection_conf)
