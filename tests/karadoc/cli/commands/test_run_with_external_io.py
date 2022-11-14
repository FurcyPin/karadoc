import shutil
import unittest
from pathlib import Path
from typing import Dict
from unittest import mock
from unittest.mock import MagicMock, call

from pyspark.sql import DataFrame

import karadoc
from karadoc.common.exceptions import ActionFileLoadingError
from karadoc.test_utils.assertion import Anything
from karadoc.test_utils.mock_settings import (
    mock_settings_for_test,
    mock_settings_for_test_class,
)
from karadoc.test_utils.spark import MockDataFrame, MockRow


@mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": "tests/resources/karadoc/cli/commands/test_run_with_external_io/model",
        "warehouse_dir": "test_working_dir/hive/warehouse",
        "connection": {
            "dummy": {
                "type": "tests.resources.connectors.dummy",
                "url": "my_url",
            }
        },
    }
)
class TestRunWithExternalIO(unittest.TestCase):
    """Test suite for all use cases around external inputs and outputs"""

    def setUp(self) -> None:
        shutil.rmtree("test_working_dir/hive", ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree("test_working_dir/hive", ignore_errors=True)

    def test_run_with_external_input(self):
        """When performing a run of a Populate with external inputs, it should call DummyConnector.read once"""

        def connector_read(connector, source: Dict):
            self.expected_df = connector.spark.sql("""SELECT "b" as b""")
            return self.expected_df

        def inspect_df(df: DataFrame):
            self.assertEqual(MockDataFrame({MockRow(b="b")}), df)

        with mock.patch(
            "tests.resources.connectors.dummy.DummyConnector.read", side_effect=connector_read, autospec=True
        ) as mock_read, mock.patch("karadoc.cli.commands.run.inspect_df", side_effect=inspect_df) as mock_inspect_df:
            karadoc.cli.run_command("run --tables test_schema.external_input")

        mock_read.assert_called_once_with(Anything, {"connection": "dummy"})
        mock_inspect_df.assert_called_once()
        self.assertTrue(Path("test_working_dir/hive/warehouse/test_schema.db/external_input").is_dir())

    def test_run_with_external_outputs(self):
        """When performing a run of a Populate with external outputs, it should call DummyConnector.write once per
        output."""
        self.actual_df = None

        def inspect_df(df: DataFrame):
            self.assertEqual(MockDataFrame({MockRow(b="b")}), df)
            self.actual_df = df

        with mock.patch("tests.resources.connectors.dummy.DummyConnector.write") as mock_write, mock.patch(
            "karadoc.cli.commands.run.inspect_df", side_effect=inspect_df
        ) as mock_inspect_df:
            karadoc.cli.run_command("run --tables test_schema.external_output")

        # First call on first output
        call_1 = call(self.actual_df, {"connection": "dummy", "test": "1"})
        call_2 = call(self.actual_df, {"connection": "dummy", "test": "2"})
        mock_write.assert_has_calls([call_1, call_2])
        mock_inspect_df.assert_called_once()

    def __template_test_with_external_input_output_overwrite(
        self, expected_df: MockDataFrame, inspect_job, command: str
    ):
        def inspect_df(df: DataFrame):
            self.assertEqual(expected_df, df)

        with mock.patch(
            "karadoc.cli.commands.run.inspect_job", side_effect=inspect_job
        ) as mock_inspect_job, mock.patch(
            "karadoc.cli.commands.run.inspect_df", side_effect=inspect_df
        ) as mock_inspect_df:
            karadoc.cli.run_command(command)

        mock_inspect_df.assert_called_once()
        mock_inspect_job.assert_called_once()

    def test_run_with_external_input_output_overwrite(self):
        """When performing a run of a Populate that overrides the read_external_input and read_external_output methods
        they should be called correctly"""

        def inspect_job(job):
            self.assertTrue(job.write_external_output_called)

        expected_df = MockDataFrame({MockRow(id=0), MockRow(id=1)})
        command = "run --tables test_schema.external_input_output_overwrite"
        self.__template_test_with_external_input_output_overwrite(expected_df, inspect_job, command)

    def test_run_with_external_inputs_outputs_overwrite(self):
        """When performing a run of a Populate that overrides the read_external_inputs and read_external_outputs methods
        they should be called correctly"""

        def inspect_job(job):
            self.assertTrue(job.write_external_outputs_called)

        expected_df = MockDataFrame({MockRow(id=0), MockRow(id=1)})
        command = "run --tables test_schema.external_inputs_outputs_overwrite"
        self.__template_test_with_external_input_output_overwrite(expected_df, inspect_job, command)

    def test_run_with_external_input_output_overwrite_with_limit_external_inputs(self):
        """When performing a run of a Populate that overrides the read_external_input and read_external_output methods
        they should be called correctly"""

        def inspect_job(job):
            self.assertTrue(job.write_external_output_called)

        expected_df = MockDataFrame({MockRow(id=0)})
        command = "run --limit-external-inputs 1 --tables test_schema.external_input_output_overwrite"
        self.__template_test_with_external_input_output_overwrite(expected_df, inspect_job, command)

    def test_run_with_external_inputs_outputs_overwrite_with_limit_external_inputs(self):
        """When performing a run of a Populate that overrides the read_external_inputs and read_external_outputs methods
        they should be called correctly"""

        def inspect_job(job):
            self.assertTrue(job.write_external_outputs_called)

        expected_df = MockDataFrame({MockRow(id=0)})
        command = "run --limit-external-inputs 1 --tables test_schema.external_inputs_outputs_overwrite"
        self.__template_test_with_external_input_output_overwrite(expected_df, inspect_job, command)

    def test_run_with_wrong_signatures(self):
        """When performing a run of a Populate that overrides either one
        of the following methods but uses the wrong signature,
        an Exception should be raised :

        - read_external_input
        - read_external_inputs
        - write_external_output
        - write_external_outputs
        """
        tables = [
            "wrong_override_signature.read_external_input",
            "wrong_override_signature.read_external_inputs",
            "wrong_override_signature.write_external_output",
            "wrong_override_signature.write_external_outputs",
        ]
        for table in tables:
            with self.assertRaises(ActionFileLoadingError) as cm:
                karadoc.cli.run_command("run --dry --tables " + table)
            the_exception = cm.exception
            self.assertIn(f"Could not load POPULATE.py file for table {table}", str(the_exception))

    def test_run_with_load_external_inputs_as_view(self):
        """When performing a run of a Populate with external inputs, it should call DummyConnector.read once"""

        def connector_read(connector, source: Dict):
            self.expected_df = connector.spark.sql("""SELECT "b" as b""")
            return self.expected_df

        def inspect_df(df: DataFrame):
            self.assertEqual(MockDataFrame({MockRow(c="b")}), df)

        with mock.patch(
            "tests.resources.connectors.dummy.DummyConnector.read", side_effect=connector_read, autospec=True
        ) as mock_read, mock.patch("karadoc.cli.commands.run.inspect_df", side_effect=inspect_df) as mock_inspect_df:
            karadoc.cli.run_command("run --tables test_schema.load_external_inputs_as_view")

        mock_read.assert_called_once_with(Anything, {"connection": "dummy"})
        mock_inspect_df.assert_called_once()
        self.assertTrue(Path("test_working_dir/hive/warehouse/test_schema.db/load_external_inputs_as_view").is_dir())

    @mock.patch("karadoc.spark.job_core.has_external_outputs.HasExternalOutputs.write_external_outputs")
    def test_run_with_validate(self, mock_write_external_outputs):
        """When using the option --validate, the number of rows of the inputs, outputs and external inputs should
        be limited to 0, and no output nor external_output should be written."""

        def connector_read(connector, source: Dict):
            return connector.spark.sql("""SELECT "a" as a""")

        def inspect_df(df: DataFrame):
            self.assertEqual(MockDataFrame([]), df)
            self.assertEqual(df.schema.simpleString(), "struct<a:string>")

        with mock.patch(
            "tests.resources.connectors.dummy.DummyConnector.read", side_effect=connector_read, autospec=True
        ) as mock_read, mock.patch("karadoc.cli.commands.run.inspect_df", side_effect=inspect_df) as mock_inspect_df:
            karadoc.cli.run_command("run --validate --tables test_schema.validate")

        mock_read.assert_called_once_with(Anything, {"connection": "dummy"})
        mock_inspect_df.assert_called_once()
        mock_write_external_outputs.assert_not_called()
        self.assertFalse(Path("test_working_dir/hive/warehouse/test_schema.db/validate").is_dir())

    @mock_settings_for_test(
        {
            "connection": {
                "dummy": {
                    "type": "tests.resources.connectors.dummy",
                    "url": "my_url",
                    "disable": "true",
                }
            }
        }
    )
    @mock.patch("karadoc.common.conf.package.get_env")
    def test_run_disabled_conn(self, get_env: MagicMock):
        """
        Given a POPULATE.py that uses a disabled connection
        When we run it
        Then an exception should be raised
        """
        get_env.return_value = "test"
        with self.assertRaises(Exception) as cm:
            karadoc.cli.run_command("run --tables test_schema.disabled_conn")
        the_exception = cm.exception
        self.assertIn("The connection test.connection.dummy is disabled", str(the_exception))

    @mock.patch("karadoc.spark.job_core.has_external_outputs.HasExternalOutputs.write_external_outputs")
    def test_run_without_no_export_option(self, mock_write_external_outputs):
        """
        Given a POPULATE.py with external_outputs
        When we run it without the no_export option
        Then it should write external outputs
        """
        karadoc.cli.run_command("run --tables test_schema.external_output")
        mock_write_external_outputs.assert_called()

    @mock.patch("karadoc.spark.job_core.has_external_outputs.HasExternalOutputs.write_external_outputs")
    def test_run_with_no_export_option(self, mock_write_external_outputs):
        """
        Given a POPULATE.py with external_outputs
        When we run it with the no_export option
        Then it should not write external outputs
        """
        karadoc.cli.run_command("run --no-export --tables test_schema.external_output")
        mock_write_external_outputs.assert_not_called()
