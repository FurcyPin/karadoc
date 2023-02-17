from typing import TYPE_CHECKING, Callable, Dict, Optional, Union

from karadoc.common.conf import CONNECTION_GROUP
from karadoc.spark.job_core.has_spark import HasSpark
from karadoc.spark.spark_connector import SparkConnector

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _read_external_input_signature_check():
    from pyspark.sql import DataFrame

    def read_external_input(source: Dict) -> DataFrame:
        """Empty method used to check the signature of the equivalent method defined in the POPULATE files"""
        pass

    return read_external_input


def _read_external_inputs_signature_check():
    from pyspark.sql import DataFrame

    def read_external_inputs() -> Dict[str, DataFrame]:
        """Empty method used to check the signature of the equivalent method defined in the POPULATE files"""
        pass

    return read_external_inputs


class HasExternalInputs(HasSpark):
    def __init__(self) -> None:
        super().__init__()
        # Private attributes
        self.__read_external_input: Optional[Callable] = None
        """Stores the optional override of the read_external_input method by the user"""
        self.__read_external_inputs: Optional[Callable] = None
        """Stores the optional override of the read_external_inputs method by the user"""

        self._limit_external_inputs: Optional[int] = None
        """When set, limits the size of every external input DataFrame to this number of row
        as soon as it is loaded"""

        # Attributes that the user may change in action files
        self.external_inputs: Dict[str, dict] = {}
        """Use this to declare your job's external inputs.
        Expected format is a dictionary of (table alias, table declaration)
        The table description must be a dict specifying the connection to use and all additional parameters specific
        to the external input. The corresponding connection must be properly configured as described in the connector's
        documentation.

        Example:
        {
            "external_input_alias": {
                "{connection}": "name_of_the_connection_used",
                "source_param_name": "source_param_value"
            }
        }
        """

    def _read_external_input_default(self, source: Dict) -> "DataFrame":
        connector = self.get_input_connector(source)
        return connector.read(source)

    def _read_external_inputs_default(self) -> Dict[str, "DataFrame"]:
        return {source_alias: self.read_external_input(source_alias) for source_alias in self.external_inputs}

    def read_external_input(self, source_alias: str) -> "DataFrame":
        """Reads a given external input and returns it as a Spark DataFrame

        :param source_alias: the alias of the source in job.external_inputs
        :return: a DataFrame
        """
        source = self.external_inputs[source_alias]
        external_input: "DataFrame"
        if self.__read_external_input is None:
            external_input = self._read_external_input_default(source)
        else:
            external_input = self.__read_external_input(source)

        if self._limit_external_inputs is not None:
            external_input = external_input.limit(self._limit_external_inputs)
        return external_input

    def read_external_inputs(self) -> Dict[str, "DataFrame"]:
        """Reads all declared external inputs and returns them as Spark DataFrames

        :return: a Dict[alias, DataFrame]
        """
        external_inputs: Dict[str, "DataFrame"]
        if self.__read_external_inputs is None:
            external_inputs = self._read_external_inputs_default()
        else:
            external_inputs = self.__read_external_inputs()

        if self._limit_external_inputs is not None:
            for alias, df in external_inputs.items():
                external_inputs[alias] = external_inputs[alias].limit(self._limit_external_inputs)
        return external_inputs

    def load_external_inputs_as_views(self, cache=False):
        for alias in self.external_inputs:
            df = self.read_external_input(alias)
            if cache:
                df = df.cache()
            df.createOrReplaceTempView(alias)

    def get_input_connector(self, source: Union[str, Dict]) -> SparkConnector:
        if type(source) == str:
            source = self.external_inputs[source]
        from karadoc.spark.spark_connector import load_connector

        return load_connector(source[CONNECTION_GROUP], self.spark)
