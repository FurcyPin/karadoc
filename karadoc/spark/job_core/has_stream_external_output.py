from typing import TYPE_CHECKING, Dict, Optional, Union

from karadoc.common.conf import CONNECTION_GROUP
from karadoc.common.job_core.package import OptionalMethod
from karadoc.spark.job_core.has_spark import HasSpark

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.streaming import DataStreamWriter


def _write_stream_external_output_signature_check():
    from pyspark.sql import DataFrame
    from pyspark.sql.streaming import DataStreamWriter

    def write_stream_external_output_signature_check(df: DataFrame, dest: Dict) -> DataStreamWriter:
        """Empty method used to check the signature of the equivalent method defined in the POPULATE files"""
        pass

    return write_stream_external_output_signature_check


class HasStreamExternalOutput(HasSpark):
    def __init__(self) -> None:
        super().__init__()

        # Private attributes
        from pyspark.sql import DataFrame
        from pyspark.sql.streaming import DataStreamWriter

        def write_external_output(df: DataFrame, dest: Dict) -> DataStreamWriter:
            connector = self.get_output_connector(dest)
            return connector.write_stream(df, dest)

        self.__write_external_output = OptionalMethod(write_external_output)

        # Attributes that the user may change
        self.external_output: Optional[dict] = None

    def write_external_output(self, df: "DataFrame", dest: Dict) -> "DataStreamWriter":
        """Writes a given DataFrame to a given external output

        :param df: The DataFrame to write
        :param dest: The alias of the external output to write to
        :return: a DataStreamWriter
        """
        return self.__write_external_output(df, dest)

    def get_output_connector(self, dest: Union[str, Dict]):
        if type(dest) == str:
            dest = self.external_output[dest]
        from karadoc.spark.spark_connector import load_connector

        return load_connector(dest[CONNECTION_GROUP], self.spark)
