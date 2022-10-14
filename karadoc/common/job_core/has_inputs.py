from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from karadoc.common import conf
from karadoc.common.job_core.has_spark import HasSpark, _table_name_to_hdfs_path
from karadoc.common.job_core.job_base import JobBase
from karadoc.common.table_utils import parse_table_name
from karadoc.common.utils.assert_utils import assert_true

WASB_SCHEMES = ["wasb", "wasbs"]
ABFS_SCHEMES = ["abfs", "abfss"]

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class HasInputs(HasSpark, JobBase, ABC):
    def __init__(self) -> None:
        super().__init__()
        JobBase.__init__(self)
        # Private attributes
        self.__inputs: Dict[str, Union[str, Dict[str, str]]] = {}
        """Stores the inputs as declared in the action file"""

        self.__standardized_inputs: Dict[str, Dict[str, str]] = {}
        """Stores the inputs after standardization,
        for instance { "alias": "schema_name.table_name" } is automatically
        transformed into { "alias": {"table": "schema_name.table_name"} }"""

        self.__input_warehouse_dir: str = conf.get_warehouse_folder_location()
        """Path to the folder where the data for all inputs managed by karadoc is stored"""

        self._limit_inputs: Optional[int] = None
        """When set, limits the size of every input DataFrame to this number of row as soon as it is loaded"""

        # Attributes that the user may change in action files
        self.hide_inputs: bool = False
        """Set to true to make inputs invisible in the `show_graph` command unless the `--show-hidden` option is used"""

    @property
    def inputs(self) -> Dict[str, dict]:
        return self.__inputs

    @inputs.setter
    def inputs(self, new_inputs: Dict[str, Union[str, dict]]):
        """Use this to declare your job's inputs.
        Expected format is a dictionary of (table alias, table declaration)
        The table declaration can either be it's full name ("schema_name.table.name")
        or a dictionary that can contain several entries:

        - table [Required]: full name of the table
        - format [Optional]: format of the table passed to spark
          (eg: orc, parquet, json, csv, text, or any custom format)
        - schema [Optional]: the Dataframe schema passed to spark, as a StructType
        - options [Optional]: a dictionary[str, str] of options

        Example:
        {
            "alias_1": "schema_name.table_name_1",
            "alias_2": {
                "table": "schema_name.table_name_2",
                "format": "csv",
                "schema": "id BIGINT, name STRING",
                "options": {"header": "false"},
            }
        }

        For the sake of keeping the code simpler table descriptions that are of type string are automatically
        transformed into dicts and stored in `standardized_inputs`.

        For example: { "alias": "schema_name.table_name" }
        is automatically transformed into { "alias": {"table": "schema_name.table_name"} }

        :param inputs:
        :return:
        """
        self.__inputs = new_inputs
        standardized_inputs = new_inputs.copy()
        for table_alias, input_table in new_inputs.items():
            if type(input_table) == str:
                standardized_inputs[table_alias] = {"table": input_table}
            elif type(input_table) == dict:
                if "table" not in input_table:
                    raise TypeError(f'job.input["{table_alias}"] is a dict, it should contain a "table" entry')
            else:
                raise TypeError(
                    f'job.input["{table_alias}"] should be a string or a dictionary, got {type(input_table)} instead.'
                )

        self.__standardized_inputs = standardized_inputs

    def list_input_tables(self) -> List[str]:
        """Return the list of full table names used as inputs for this job.

        :return: a list of string
        """
        return [input_table["table"] for input_table in self.__standardized_inputs.values()]

    @abstractmethod
    def get_reader(self):
        """Return a :class:`DataStreamReader` Or `DataFrameReader` that can be used to read data"""
        pass

    def read_table(self, table_alias: str, schema=None) -> "DataFrame":
        """Read a table based on its alias

        :param table_alias: the alias of the table following this pattern: db_name.table_name
        :param schema: Optional, The schema used to read the data
        :return: a spark Dataframe
        """
        input_table = self.__standardized_inputs[table_alias]
        if schema is None:
            schema = input_table.get("schema")
        input_format = input_table.get("format")
        read_options = input_table.get("options")
        return self._read_input_table(input_table["table"], schema, input_format, read_options)

    def _read_input_table(
        self, table_full_name: str, schema=None, input_format: str = None, read_options: dict = None
    ) -> "DataFrame":
        """Read a table based on its full name.

        End-users should NOT use this method directly.
        Instead, they MUST properly declare their `job.inputs` and use `read_table`.
        Not declaring `job.inputs` in a POPULATE would break the data lineage feature and all the beautiful stuff.
        This method can however be used in unit tests or for throw-away investigation code.

        :param table_full_name: the full name of the table
        :param schema: Optional, the schema used to read the data
        :param input_format: Optional, specify the format of the input table
                       (by default, the format declared in the input table's POPULATE is used)
        :param read_options: Optional, options passed to the DataFrameReader.
        :return: a spark Dataframe
        """
        from karadoc.common.run import load_populate
        from karadoc.common.stream import load_stream_file

        if input_format is None:
            try:
                input_job = load_populate(table_full_name)
            except FileNotFoundError:
                input_job = load_stream_file(table_full_name)
            input_format = input_job.output_format

        df = self._read_path(self.hdfs_input(table_full_name), input_format, schema, read_options)

        if self._limit_inputs is not None:
            df = df.limit(self._limit_inputs)

        return df

    def hdfs_input(self, table_name: str, remote_env_name: str = None) -> str:
        (schema_name, table_name, partition) = parse_table_name(table_name)
        if remote_env_name is not None:
            remote_input_warehouse_dir = self.__get_input_warehouse_dir_for_remote_env(remote_env_name)
        else:
            remote_input_warehouse_dir = self.__input_warehouse_dir
        return _table_name_to_hdfs_path(remote_input_warehouse_dir, schema_name, table_name, partition)

    def list_hdfs_inputs(self) -> List[str]:
        return [self.hdfs_input(table) for table in self.list_input_tables()]

    def _read_path(self, table_path, input_format, schema=None, options=None):
        assert_true(input_format is not None)

        df_reader = self.get_reader()
        if schema is not None:
            df_reader = df_reader.schema(schema)
        if options is not None:
            df_reader = df_reader.options(**options)

        df_reader = df_reader.format(input_format)
        df = df_reader.load(table_path)
        return df

    def load_inputs_as_views(self, cache=False):
        for alias, full_table_name in self.__inputs.items():
            df = self.read_table(alias)
            if cache:
                df = df.cache()
            df.createOrReplaceTempView(alias)

    def _configure_remote_input(self, remote_env_name: str):
        """Configure the job to read from a remote warehouse by modifying `job.__input_warehouse_dir`.

        :param remote_env_name:
        :return:
        """
        remote_input_warehouse_dir = self.__get_input_warehouse_dir_for_remote_env(remote_env_name)
        self.__input_warehouse_dir = remote_input_warehouse_dir

    def __get_input_warehouse_dir_for_remote_env(self, remote_env_name):
        remote_env = conf.get_remote_env_conf(remote_env_name)
        remote_env_type = remote_env["type"]
        if remote_env_type == "default":
            remote_input_warehouse_dir = remote_env["warehouse_dir"]
        elif remote_env_type in WASB_SCHEMES:
            storage_account = remote_env["storage_account"]
            storage_key = remote_env["storage_key"]
            container_name = remote_env["container_name"]
            warehouse_dir = remote_env["warehouse_dir"]
            self.spark.conf.set(
                "fs.azure.account.keyprovider.%s.blob.core.windows.net" % storage_account,
                "org.apache.hadoop.fs.azure.SimpleKeyProvider",
            )
            self.spark.conf.set("fs.azure.account.key.%s.blob.core.windows.net" % storage_account, storage_key)
            remote_input_warehouse_dir = "wasbs://%s@%s.blob.core.windows.net/%s" % (
                container_name,
                storage_account,
                warehouse_dir,
            )
        elif remote_env_type in ABFS_SCHEMES:
            storage_account = remote_env["storage_account"]
            container_name = remote_env["container_name"]
            warehouse_dir = remote_env["warehouse_dir"]
            remote_input_warehouse_dir = "abfss://%s@%s.dfs.core.windows.net/%s" % (
                container_name,
                storage_account,
                warehouse_dir,
            )
        else:
            raise NotImplementedError(
                f"env type {remote_env_type} is not supported, supported env type are {WASB_SCHEMES + ABFS_SCHEMES}"
            )
        return remote_input_warehouse_dir
