from typing import TYPE_CHECKING, Dict, Union

from karadoc.common.conf import CONNECTION_GROUP
from karadoc.common.job_core.has_spark import HasSpark

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _read_stream_external_input_signature_check():
    from pyspark.sql import DataFrame

    def read_stream_external_input(source: Dict) -> DataFrame:
        """Empty method used to check the signature of the equivalent method defined in the POPULATE files"""
        pass

    return read_stream_external_input


def _read_stream_external_inputs_signature_check():
    from pyspark.sql import DataFrame

    def read_stream_external_inputs() -> Dict[str, DataFrame]:
        """Empty method used to check the signature of the equivalent method defined in the POPULATE files"""
        pass

    return read_stream_external_inputs


class HasStreamExternalInputs(HasSpark):
    def __init__(self) -> None:
        super().__init__()
        # Private attributes
        self.__read_external_input = None
        self.__read_external_inputs = None

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
                "connection": "name_of_the_connection_used",
                "source_param_name": "source_param_value"
            }
        }
        """

    def _read_external_input_default(self, source: Dict) -> "DataFrame":
        connector = self.get_input_connector(source)
        return connector.read_stream(source)

    def _read_external_inputs_default(self) -> Dict[str, "DataFrame"]:
        return {source_alias: self.read_external_input(source_alias) for source_alias in self.external_inputs}

    def read_external_input(self, source_alias: str) -> "DataFrame":
        """Reads a given external input and returns it as a Spark DataFrame

        :param source_alias: the alias of the source in job.external_inputs
        :return: a DataFrame
        """
        source = self.external_inputs[source_alias]
        if self.__read_external_input is None:
            return self._read_external_input_default(source)
        else:
            return self.__read_external_input(source)

    def read_external_inputs(self) -> Dict[str, "DataFrame"]:
        """Reads all declared external inputs and returns them as Spark DataFrames

        :return: a Dict[alias, DataFrame]
        """
        if self.__read_external_inputs is None:
            return self._read_external_inputs_default()
        else:
            return self.__read_external_inputs()

    def load_external_inputs_as_views(self, cache=False):
        for alias in self.external_inputs:
            df = self.read_external_input(alias)
            if cache:
                df = df.cache()
            df.createOrReplaceTempView(alias)

    def get_input_connector(self, source: Union[str, Dict]):
        if type(source) == str:
            source = self.external_inputs[source]
        from karadoc.common.connector import load_connector

        return load_connector(source[CONNECTION_GROUP], self.spark)
