import os
import shutil
from typing import Dict
from unittest import mock
from unittest.mock import MagicMock

from pyspark.sql import DataFrame

import karadoc
from karadoc.common import conf
from karadoc.common.exceptions import ActionFileLoadingError
from karadoc.spark.stream_utils import batch_to_stream, stream_to_batch
from karadoc.test_utils.assertion import Anything
from karadoc.test_utils.mock_settings import mock_settings_for_test, mock_settings_for_test_class
from karadoc.test_utils.pyspark_test_class import PySparkTest
from karadoc.test_utils.spark import MockDataFrame, MockRow
from tests.karadoc.test_utils import get_resource_folder_path

# For some reason, when applied on the class, a patch decorator will NOT be applied to the setUp and tearDown functions
# For this reason, we define it here once, then apply it to the class AND the setUp and tearDown methods
config_mock = mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": get_resource_folder_path(__name__) + "/model",
        "spark_stream_dir": "test_working_dir/spark/stream",
        "connection": {"dummy": {"type": "tests.resources.spark.connectors.dummy"}},
        "warehouse_dir": "test_working_dir/hive/warehouse",
    }
)


@config_mock
class TestStreamWithExternalIO(PySparkTest):
    """Test suite for all use cases around external inputs and output"""

    @config_mock
    def setUp(self) -> None:
        shutil.rmtree(conf.get_warehouse_folder_location(), ignore_errors=True)
        shutil.rmtree(conf.get_streaming_checkpoint_folder_location(), ignore_errors=True)
        shutil.rmtree(conf.get_spark_stream_tmp_dir(), ignore_errors=True)

    @config_mock
    def tearDown(self) -> None:
        shutil.rmtree(conf.get_warehouse_folder_location(), ignore_errors=True)
        shutil.rmtree(conf.get_streaming_checkpoint_folder_location(), ignore_errors=True)
        shutil.rmtree(conf.get_spark_stream_tmp_dir(), ignore_errors=True)

    def test_stream_with_external_input(self):
        """When we execute a stream with external inputs, it should call DummyConnector.read"""

        def inspect_df(df: DataFrame):
            self.assertEqual(MockDataFrame([MockRow(a="a")]), stream_to_batch(df))

        with mock.patch("karadoc.cli.commands.stream.inspect_df", side_effect=inspect_df) as mock_inspect_df:
            karadoc.cli.run_command("stream --tables test_schema.external_input --streaming-mode once")
        mock_inspect_df.assert_called_once()

    def test_stream_with_external_input_read_stream_called_once(self):
        """When we execute a stream with external inputs, it should call DummyConnector.read once"""

        def connector_read(connector, source: Dict):
            df = connector.spark.sql("""SELECT "b" as b""")
            return batch_to_stream(df)

        def inspect_df(df: DataFrame):
            self.assertEqual(MockDataFrame([MockRow(b="b")]), stream_to_batch(df))

        with mock.patch(
            "tests.resources.spark.connectors.dummy.DummyConnector.read_stream",
            side_effect=connector_read,
            autospec=True,
        ) as mock_read, mock.patch("karadoc.cli.commands.stream.inspect_df", side_effect=inspect_df) as mock_inspect_df:
            karadoc.cli.run_command("stream --tables test_schema.external_input --streaming-mode once")

        mock_read.assert_called_once_with(Anything, {"connection": "dummy", "table": "external_input_test_table"})
        mock_inspect_df.assert_called_once()

    def test_stream_with_external_output(self):
        """When we execute a stream with external output, we should call the write_stream method of the connector
        declared in the external_outputs"""
        self.actual_df = None

        def inspect_df(df: DataFrame):
            df_processed = stream_to_batch(df)
            self.assertEqual(MockDataFrame([MockRow(b="b")]), df_processed)
            self.actual_df = df_processed

        with mock.patch("tests.resources.spark.connectors.dummy.DummyConnector.write_stream") as mock_write, mock.patch(
            "karadoc.cli.commands.stream.inspect_df", side_effect=inspect_df
        ) as mock_inspect_df:
            karadoc.cli.run_command("stream --tables test_schema.external_output --streaming-mode once")

        mock_write.assert_called_once()
        mock_inspect_df.assert_called_once()

    def test_stream_with_external_input_output_overwrite(self):
        """When we execute a stream that overrides the read_external_input and write_external_output methods
        they should be called correctly"""

        def inspect_df(df: DataFrame):
            df_processed = stream_to_batch(df)
            self.assertEqual(MockDataFrame([MockRow(b="b")]), df_processed)

        def inspect_job(job):
            self.assertTrue(job.write_external_output_called)

        with mock.patch(
            "karadoc.cli.commands.stream.inspect_job", side_effect=inspect_job
        ) as mock_inspect_job, mock.patch(
            "karadoc.cli.commands.stream.inspect_df", side_effect=inspect_df
        ) as mock_inspect_df:
            karadoc.cli.run_command("stream --tables test_schema.external_input_output_overwrite --streaming-mode once")

        mock_inspect_df.assert_called_once()
        mock_inspect_job.assert_called_once()

    def test_stream_with_external_inputs_output_overwrite(self):
        """When we execute a stream that overrides the read_external_inputs and write_external_output methods
        they should be called correctly"""

        def inspect_df(df: DataFrame):
            df_processed = stream_to_batch(df)
            self.assertEqual(MockDataFrame([MockRow(b="b")]), df_processed)

        def inspect_job(job):
            self.assertTrue(job.write_external_output_called)

        with mock.patch(
            "karadoc.cli.commands.stream.inspect_job", side_effect=inspect_job
        ) as mock_inspect_job, mock.patch(
            "karadoc.cli.commands.stream.inspect_df", side_effect=inspect_df
        ) as mock_inspect_df:
            karadoc.cli.run_command(
                "stream --tables test_schema.external_inputs_output_overwrite --streaming-mode once"
            )

        mock_inspect_df.assert_called_once()
        mock_inspect_job.assert_called_once()

    def test_stream_with_wrong_signatures(self):
        """When we execute a stream that overrides either one of the following methods but uses the wrong signature,
        an Exception should be raised :

        - read_external_input
        - read_external_inputs
        - write_external_output
        """
        tables = [
            "wrong_override_signature.read_external_input",
            "wrong_override_signature.read_external_inputs",
            "wrong_override_signature.write_external_output",
        ]
        for table in tables:
            with self.assertRaises(ActionFileLoadingError) as cm:
                karadoc.cli.run_command("stream --dry --tables " + table)
            the_exception = cm.exception
            self.assertIn(f"Could not load STREAM.py file for table {table}", str(the_exception))
            self.assertIn("should have the following signature", str(the_exception.__cause__))

    def test_stream_checkpointing(self):
        """When we execute a stream the checkpointing should work well."""
        output_tmp_path = os.path.join(conf.get_spark_stream_tmp_dir(), "test_external_output.table")

        karadoc.cli.run_command(
            "stream --tables test_schema.checkpointing_activated --streaming-mode once --vars var=1"
        )
        stream_output = self.spark.read.load(output_tmp_path)
        self.assertEqual(MockDataFrame([MockRow(a=1)]), stream_output)
        karadoc.cli.run_command(
            "stream --tables test_schema.checkpointing_activated --streaming-mode once --vars var=2"
        )
        stream_output = self.spark.read.load(output_tmp_path)
        self.assertEqual(MockDataFrame([MockRow(a=1), MockRow(a=2)]), stream_output)

    @mock_settings_for_test(
        {
            "connection": {
                "dummy": {
                    "type": "tests.resources.spark.connectors.dummy",
                    "url": "my_url",
                    "disable": "true",
                }
            }
        }
    )
    @mock.patch("karadoc.common.conf.package.get_env")
    def test_stream_with_disabled_conn(self, get_env: MagicMock):
        """
        Given a STREAM.py that uses a disabled connection
        When we run it
        Then an exception should be raised
        """
        get_env.return_value = "test"
        with self.assertRaises(Exception) as cm:
            karadoc.cli.run_command("stream --tables test_schema.disabled_conn")
        the_exception = cm.exception
        self.assertIn("The connection test.connection.dummy is disabled", str(the_exception))
