from typing import Dict, Optional, Union

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

from karadoc.common.conf import CONNECTION_GROUP


def _write_stream_external_output_signature_check(df: DataFrame, dest: Dict) -> DataStreamWriter:
    """We uses this empty method when to check the signature of the equivalent method defined in the STREAM files"""
    pass


class HasStreamExternalOutput:
    def __init__(self) -> None:
        # Attributes that the user may change
        self.external_output: Optional[dict] = None

        # Attributes that the user is not supposed to change
        self.__write_external_output = None

    def _write_external_output_default(self, df: DataFrame, dest: Dict) -> DataStreamWriter:
        connector = self.get_output_connector(dest)
        return connector.write_stream(df, dest)

    def write_external_output(self, df: DataFrame, dest: Dict) -> DataStreamWriter:
        """Writes a given DataFrame to a given external output

        :param df: The DataFrame to write
        :param dest: The alias of the external output to write to
        :return: nothing
        """
        if self.__write_external_output is None:
            return self._write_external_output_default(df, dest)
        else:
            return self.__write_external_output(df, dest)

    def get_output_connector(self, dest: Union[str, Dict]):
        if type(dest) == str:
            dest = self.external_output[dest]
        from karadoc.common.connector import load_connector

        return load_connector(dest[CONNECTION_GROUP], self.spark)
