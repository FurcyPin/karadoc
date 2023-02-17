import os
from typing import Dict

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter

from karadoc.common.conf import ConfBox
from karadoc.common.conf.configurable_class import ConfParam
from karadoc.common.table_utils import parse_table_name
from karadoc.common.utils.assert_utils import assert_true
from karadoc.spark.spark_connector import ConfigurableSparkConnector


def _get_blob_location(bucket_name: str, table_name: str) -> str:
    (schema_name, table_name, _) = parse_table_name(table_name)
    return "gs://%s/tmp/big-query/%s.db/%s" % (bucket_name, schema_name, table_name)


class BigQueryConnector(ConfigurableSparkConnector):
    project_name = ConfParam(
        required=True, description="Name of the bigquery project where the source/destination table is located."
    )
    parent_project = ConfParam(
        default=lambda conn: conn.project_name.get(), description="Name of the project used for billing."
    )
    bucket_name = ConfParam(
        required=False,
        description="(Required for writing) Name of the intermediary Google Storage bucket into which "
        "Spark will write data before BigQuery ingests them. The used service account "
        "must have write access rights on this bucket.",
    )
    json_keyfile = ConfParam(required=True, description="Path to the json file containing GCP authentication secrets.")
    materialization_dataset = ConfParam(
        description="(Required for reading views or custom queries) "
        "Name of the dataset where temporary BigQuery results will be written. "
        "The used service account must have write access rights on this dataset."
    )
    materialization_project = ConfParam(
        description="(Required for reading views or custom queries) "
        "Name of the project where temporary BigQuery results will be written. "
        "The used service account must have write access rights on this dataset."
    )

    def __init__(self, spark: SparkSession, connection_conf: ConfBox):
        ConfigurableSparkConnector.__init__(self, spark, connection_conf)

    def __get_json_keyfile_path(self):
        json_keyfile_path = self.json_keyfile.get()
        assert_true(os.path.isfile(json_keyfile_path), "File %s not found" % json_keyfile_path)
        return json_keyfile_path

    def __get_read_options(self) -> Dict[str, str]:
        json_keyfile_path = self.__get_json_keyfile_path()
        read_options = {
            "project": self.project_name.get(),
            "parentProject": self.parent_project.get(),
            "credentialsFile": json_keyfile_path,
        }
        if self.materialization_project.is_defined() and self.materialization_dataset.is_defined():
            read_options.update(
                **{
                    "viewsEnabled": "true",
                    "materializationDataset": self.materialization_dataset.get(),
                    "materializationProject": self.materialization_project.get(),
                }
            )

        return read_options

    def __get_write_options(self, dest) -> Dict[str, str]:
        json_keyfile_path = self.__get_json_keyfile_path()
        write_options = {
            "project": self.project_name.get(),
            "parentProject": self.parent_project.get(),
            "intermediateFormat": dest.get("loading_format", "parquet"),
            "temporaryGcsBucket": self.bucket_name.get(required=True),
            "credentialsFile": json_keyfile_path,
        }
        gs_options = {
            "fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            "google.cloud.auth.service.account.enable": "true",
            "fs.gs.auth.service.account.enable": "true",
            "fs.gs.auth.service.account.json.keyfile": json_keyfile_path,
        }
        for k, v in gs_options.items():
            self.spark._jsc.hadoopConfiguration().set(k, v)
        return write_options

    def __get_bigquery_client(self) -> bigquery.Client:
        credentials = service_account.Credentials.from_service_account_file(self.__get_json_keyfile_path())
        return bigquery.Client(project=self.project_name.get(), credentials=credentials)

    @staticmethod
    def __is_an_existing_table(client: bigquery.Client, table_name: str) -> bool:
        try:
            client.get_table(table_name)
            return True
        except NotFound:
            return False

    def write(self, df: DataFrame, dest):
        """Write the given DataFrame to an external output on Google BigQuery via the
        `spark-bigquery connector <https://github.com/GoogleCloudPlatform/spark-bigquery-connector>`_

        The destination must have the following attributes:

        - "table": full name of the destination table (e.g. "dataset_name.table_name")
        - "mode": Loading mode used by spark:

          - overwrite: erase previously existing data (recommended because idempotent)
          - append: append to previously existing data (useful for incremental insert but not idempotent)
          - errorifexists: fail if there is previously existing data (not idempotent)
          - ignore: do nothing if there is previously existing data (not idempotent)
        - "loading_format": (Optional) Format used to write the data on google storage before loading into BigQuery:

          - parquet: (default) Arrays can be null but will be wrapped with ".list.element" fields
          - orc: Null arrays will be converted to empty arrays (less naming boilerplate but you won't be able to tell
            the difference between empty and missing arrays)
          - avro: No extra ".list.element" boilerplate like parquet, but timestamps are converted to Integers.
        - "expiring_date": (Unsupported atm) Datetime in UTC at which the table will be deleted
        - "options": (Optional[Dict[str, str]]) options that can be passed to the bigquery reader as a Dict[str, str]
          some examples `here <https://github.com/GoogleCloudPlatform/spark-bigquery-connector>`_.

        The corresponding connection must have the following attributes:

        - type = 'bigquery'
        - bucket_name = name of the Google Storage bucket to use to write the intermediate temporary file.
        - project_name = name of the BigQuery project where the destination table is located
        - json_keyfile = path to the json file containing the BigQuery credentials

        :param df: the DataFrame to export
        :param dest: A dictionary describing the destination
        :return:
        """
        write_options = self.__get_write_options(dest)
        df.write.format("bigquery").mode(dest["mode"]).options(**write_options).options(**dest.get("options", {})).save(
            dest["table"]
        )

    def write_stream(self, stream_df: DataFrame, dest) -> DataStreamWriter:
        """Write the given stream DataFrame to an external output on Google BigQuery in streaming via the
        `spark-bigquery connector <https://github.com/GoogleCloudPlatform/spark-bigquery-connector>`_

        The destination must have the following attributes:

        - "table": full name of the destination table (e.g. "dataset_name.table_name")
        - "loading_format": (Optional) Format used to write the data on google storage before loading into BigQuery:

          - parquet: (default) Arrays can be null but will be wrapped with ".list.element" fields
          - orc: Null arrays will be converted to empty arrays (less naming boilerplate but you won't be able to tell
            the difference between empty and missing arrays)
          - avro: No extra ".list.element" boilerplate like parquet, but timestamps are converted to Integers.
        - "expiring_date": (Unsupported atm) Datetime in UTC at which the table will be deleted
        - "options": (Optional[Dict[str, str]]) options that can be passed to the bigquery reader as a Dict[str, str]
          some examples `here <https://github.com/GoogleCloudPlatform/spark-bigquery-connector>`_.

        The corresponding connection must have the following attributes:

        - type = 'bigquery'
        - project_name = name of the BigQuery project where the destination table is located
        - bucket_name = name of the Google Storage bucket to use to write the intermediate temporary file.
        - json_keyfile = path to the json file containing the BigQuery credentials

        :param stream_df: the DataFrame to export
        :param dest: A dictionary describing the destination
        :return:
        """
        write_options = self.__get_write_options(dest)
        return (
            stream_df.writeStream.format("bigquery")
            .options(**write_options)
            .options(**dest.get("options", {}))
            .option("table", dest["table"])
        )

    def read(self, source: dict) -> DataFrame:
        """Read data from a Big Query table via the
        `spark-bigquery connector <https://github.com/GoogleCloudPlatform/spark-bigquery-connector>`_

        The source must have the following attributes:

        - "connection": name of the bigquery connection
        - "table": name to the table to be read
        - "query": (Optional) execute the provided query on BigQuery and returns the result.
          if provided, "table" will be ignored but we recommend to still fill it for documentation purposes.
        - "options": (Optional[Dict[str, str]]) options that can be passed to the bigquery reader as a Dict[str, str]
          some examples `here <https://github.com/GoogleCloudPlatform/spark-bigquery-connector>`_.

        The corresponding connection must have the following attributes:

        - type = 'bigquery'
        - project_name = name of the bigquery project where the source table is located
        - bucket_name = name of the bigquery bucket (It is only used for write method)
        - json_keyfile = file of bigquery connection configuration

        :param source: a dictionary describing the source
        :return: DataFrame
        """
        read_options = self.__get_read_options()
        spark_reader = self.spark.read.format("bigquery").options(**read_options)
        spark_reader = spark_reader.options(**source.get("options", {}))

        if source.get("query") is not None:
            self.materialization_dataset.require()
            self.materialization_project.require()
            spark_reader = spark_reader.option("query", source["query"])
        else:
            spark_reader = spark_reader.option("table", source["table"])

        df = spark_reader.load()
        return df

    def remove_partition(self, partition: str, dest):
        """
        The destination must have the following attributes:

        - "connection": name of the bigquery connection
        - "table": name to the table to be read
        - "options": (Optional[Dict[str, str]]) options that can be passed to the bigquery reader as a Dict[str, str]
          some examples `here <https://github.com/GoogleCloudPlatform/spark-bigquery-connector>`_.

        The corresponding connection must have the following attributes:

        - type = 'bigquery'
        - project_name = name of the bigquery project where the source table is located
        - bucket_name = name of the bigquery bucket (It is only used for write method)
        - json_keyfile = file of bigquery connection configuration

        Documentation to delete a partition
        `here <https://cloud.google.com/bigquery/docs/managing-partitioned-tables#delete_a_partition>`_.

        :param partition: The BigQuery partition to remove, express as string, e.g for a DAY: YYYYMMDD
        :param dest: A dictionary describing the destination
        :return:
        """

        bq = self.__get_bigquery_client()
        if self.__is_an_existing_table(bq, dest["table"]):
            bq.delete_table("{table}${partition}".format(table=dest["table"], partition=partition))
        bq.close()
