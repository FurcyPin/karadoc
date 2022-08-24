import shutil
import unittest
from unittest import mock

import karadoc
from karadoc.common import conf
from karadoc.common.exceptions import ActionFileLoadingError
from karadoc.test_utils.mock_settings import (
    mock_settings_for_test,
    mock_settings_for_test_class,
)

# For some reason, when applied on the class, a patch decorator will NOT be applied to the setUp and tearDown functions
# For this reason, we define it here once, then apply it to the class AND the setUp and tearDown methods
from karadoc.test_utils.stdio import captured_output

config_mock = mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": "tests/resources/karadoc/cli/commands/test_stream/model",
        "spark_stream_dir": "test_working_dir/spark/stream",
        "connection": {"dummy": {"type": "tests.resources.connectors.dummy"}},
    }
)


@config_mock
class TestStream(unittest.TestCase):
    @config_mock
    def setUp(self) -> None:
        shutil.rmtree(conf.get_spark_stream_tmp_dir(), ignore_errors=True)

    @config_mock
    def tearDown(self) -> None:
        shutil.rmtree(conf.get_spark_stream_tmp_dir(), ignore_errors=True)

    def test_dry_stream(self):
        karadoc.cli.run_command("stream --dry --tables test_schema.test_table --streaming-mode once")

    def test_stream(self):
        karadoc.cli.run_command("stream --tables test_schema.test_table --streaming-mode once")

    def test_stream_with_missing_vars(self):
        """When running in distributed mode, if a var is declared in the STREAM.py file and is not
        passed in the arguments, the stream command shall fail.
        """
        with self.assertRaises(Exception) as cm:
            karadoc.cli.run_command("stream --dry --tables test_schema.var_table --streaming-mode once")
        the_exception = cm.exception
        self.assertEqual("The following variables are required and missing: [day]", str(the_exception.__cause__))

    @mock_settings_for_test(
        {
            "allow_missing_vars": True,
        }
    )
    def test_stream_with_allow_missing_vars(self):
        """When running in distributed mode, if a var is declared in the STREAM.py file and is not
        passed in the arguments, the stream command shall fail.
        """

        def inspect_job(job):
            self.assertEqual(job.vars, {"day": "2018-01-01"})

        with mock.patch("karadoc.cli.commands.stream.inspect_job", side_effect=inspect_job) as check_mock:
            karadoc.cli.run_command("stream --dry --tables test_schema.var_table --streaming-mode once")
        check_mock.assert_called_once()

    def test_run_with_vars(self):
        def inspect_job(job):
            self.assertEqual(job.vars, {"day": "2018-02-02"})

        with mock.patch("karadoc.cli.commands.stream.inspect_job", side_effect=inspect_job) as check_mock:
            karadoc.cli.run_command(
                "stream --dry --vars day=2018-02-02 --tables test_schema.var_table --streaming-mode once"
            )
        check_mock.assert_called_once()

    def test_stream_with_job_init_invalid(self):
        """When running a STREAM that calls job.init(), it should raise an exception."""
        with self.assertRaises(ActionFileLoadingError) as cm:
            karadoc.cli.run_command("stream --tables test_schema.job_init_invalid --streaming-mode once")
        the_exception = cm.exception
        self.assertIn("Could not load STREAM file for table test_schema.job_init_invalid", str(the_exception))
        self.assertEqual(
            "Error: the spark context should not be initialized in an action file.", str(the_exception.__cause__)
        )

    @mock.patch("karadoc.common.job_core.has_stream_external_output.HasStreamExternalOutput.write_external_output")
    def test_stream_without_no_export_option(self, mock_write_external_output):
        """
        Given a STREAM.py with an external_output
        When we run it without the no_export option
        Then it should write the external output
        """
        karadoc.cli.run_command("stream --tables test_schema.table_test_no_export_option --streaming-mode once")
        mock_write_external_output.assert_called()

    @mock.patch("karadoc.common.job_core.has_stream_external_output.HasStreamExternalOutput.write_external_output")
    def test_stream_with_no_export_option(self, mock_write_external_output):
        """
        Given a STREAM.py with an external_output
        When we run it with the no_export option
        Then it should not write the external output
        """
        karadoc.cli.run_command(
            "stream --no-export --tables test_schema.table_test_no_export_option --streaming-mode once"
        )
        mock_write_external_output.assert_not_called()

    def test_run_disabled_job(self):
        """
        Given a STREAM.py with a disabled job
        When we run it
        Then an exception should be raised
        """
        with self.assertRaises(Exception) as cm:
            karadoc.cli.run_command("stream --tables test_schema.disabled_job --streaming-mode once")
        the_exception = cm.exception
        self.assertEqual("The job has been disabled, it should not be launched", str(the_exception))

    def test_dry_run_disabled_job(self):
        """
        Given a STREAM.py with a disabled job
        When we run it in dry mode
        Then a warning should be raised
        """
        with captured_output() as (out, err):
            karadoc.cli.run_command("stream --dry --tables test_schema.disabled_job --streaming-mode once")
        self.assertIn("WARN: The job has been disabled, it should not be launched", out.getvalue())
