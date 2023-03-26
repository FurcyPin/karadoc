from typing import TYPE_CHECKING, Dict, Union

from karadoc.common.conf import CONNECTION_GROUP
from karadoc.common.job_core.package import OptionalMethod
from karadoc.spark.job_core.has_spark import HasSpark

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class HasExternalOutputs(HasSpark):
    def __init__(self) -> None:
        super().__init__()

        # Private attributes
        from pyspark.sql import DataFrame

        def write_external_output(df: DataFrame, dest: Dict) -> None:
            connector = self.get_output_connector(dest)
            connector.write(df, dest)

        def write_external_outputs(df: DataFrame) -> None:
            for _, dest in self.external_outputs.items():
                self.write_external_output(df, dest)

        self.__write_external_output = OptionalMethod(write_external_output)
        self.__write_external_outputs = OptionalMethod(write_external_outputs)

        # Attributes that the user may change
        self.external_outputs: Dict[str, dict] = {}

    def write_external_output(self, df: "DataFrame", dest: Dict) -> None:
        """Writes a given DataFrame to a given external output

        :param df: The DataFrame to write
        :param dest: The alias of the external output to write to
        :return: nothing
        """
        return self.__write_external_output(df, dest)

    def write_external_outputs(self, df: "DataFrame") -> None:
        """Writes a given DataFrame to all the declared external outputs

        :param df: The DataFrame to write
        :return: nothing
        """
        return self.__write_external_outputs(df)

    def get_output_connector(self, dest: Union[str, Dict]):
        if type(dest) == str:
            dest = self.external_outputs[dest]
        from karadoc.spark.spark_connector import load_connector

        return load_connector(dest[CONNECTION_GROUP], self.spark)
