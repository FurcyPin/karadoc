import os
import shutil
import unittest
from pathlib import Path
from unittest import mock

from pyspark.sql import DataFrame

import karadoc
from karadoc.common import conf
from karadoc.common.exceptions import ActionFileLoadingError
from karadoc.common.stream_utils import stream_to_batch
from karadoc.test_utils.mock_settings import (
    mock_settings_for_test,
    mock_settings_for_test_class,
)
from karadoc.test_utils.spark import MockDataFrame, MockRow

# For some reason, when applied on the class, a patch decorator will NOT be applied to the setUp and tearDown functions
# For this reason, we define it here once, then apply it to the class AND the setUp and tearDown methods
from karadoc.test_utils.stdio import captured_output
from tests.karadoc.test_utils import get_resource_folder_path

warehouse_dir = "test_working_dir/hive/warehouse"

config_mock = mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": get_resource_folder_path(__name__) + "/model",
        "spark_stream_dir": "test_working_dir/spark/stream",
        "connection": {"dummy": {"type": "tests.resources.connectors.dummy"}},
        "warehouse_dir": warehouse_dir,
    }
)


@config_mock
class TestStream(unittest.TestCase):
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
        self.assertIn("Could not load STREAM.py file for table test_schema.job_init_invalid", str(the_exception))
        self.assertEqual(
            "Error: the spark context should not be initialized in an action file.", str(the_exception.__cause__)
        )

    @mock.patch("karadoc.spark.job_core.has_stream_external_output.HasStreamExternalOutput.write_external_output")
    def test_stream_without_no_export_option(self, mock_write_external_output):
        """
        Given a STREAM.py with an external_output
        When we run it without the no_export option
        Then it should write the external output
        """
        karadoc.cli.run_command("stream --tables test_schema.table_test_no_export_option --streaming-mode once")
        mock_write_external_output.assert_called()

    @mock.patch("karadoc.spark.job_core.has_stream_external_output.HasStreamExternalOutput.write_external_output")
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

    def test_stream_with_partitions(self):
        karadoc.cli.run_command(
            "stream --vars day=2018-02-02 --tables test_schema.partition_table " "--streaming-mode once"
        )
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/partition_table/day=2018-02-02").is_dir())

    def test_stream_with_dynamic_partitions(self):
        karadoc.cli.run_command("stream --tables test_schema.dynamic_partition_only --streaming-mode once")
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/dynamic_partition_only/day=2018-01-01/BU=US").is_dir())
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/dynamic_partition_only/day=2018-01-01/BU=FR").is_dir())
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/dynamic_partition_only/day=2018-01-02/BU=US").is_dir())
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/dynamic_partition_only/day=2018-01-02/BU=FR").is_dir())
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/dynamic_partition_only/day=2018-01-03/BU=US").is_dir())

    def test_stream_with_static_and_dynamic_partitions(self):
        karadoc.cli.run_command(
            "stream --vars day=2018-01-01 --tables test_schema.static_and_dynamic_partition " "--streaming-mode once"
        )
        self.assertTrue(
            Path(f"{warehouse_dir}/test_schema.db/static_and_dynamic_partition/day=2018-01-01/BU=US/test=2").is_dir()
        )
        self.assertTrue(
            Path(f"{warehouse_dir}/test_schema.db/static_and_dynamic_partition/day=2018-01-01/BU=FR/test=1").is_dir()
        )
        self.assertTrue(
            Path(f"{warehouse_dir}/test_schema.db/static_and_dynamic_partition/day=2018-01-01/BU=FR/test=2").is_dir()
        )

    def test_run_with_write_options_set_in_populate(self):
        karadoc.cli.run_command("stream --tables test_schema.with_write_options " "--streaming-mode once")
        output_files = os.listdir(f"{warehouse_dir}/test_schema.db/with_write_options/")
        is_gzip = any([x.endswith("gz") for x in output_files])
        self.assertTrue(is_gzip)

    @mock_settings_for_test({"spark.write.options.json": {"compression": "gzip"}})
    def test_run_write_options_in_settings(self):
        karadoc.cli.run_command("stream --tables test_schema.without_write_options " "--streaming-mode once")
        output_files = os.listdir(f"{warehouse_dir}/test_schema.db/without_write_options/")
        is_gzip = any([x.endswith("gz") for x in output_files])
        self.assertTrue(is_gzip)

    @mock_settings_for_test({"spark.write.options.json": {"compression": "gzip"}})
    def test_run_write_options_in_settings_and_populate(self):
        karadoc.cli.run_command("stream --tables test_schema.with_write_options " "--streaming-mode once")
        output_files = os.listdir(f"{warehouse_dir}/test_schema.db/with_write_options/")
        is_gzip = any([x.endswith("gz") for x in output_files])
        self.assertTrue(is_gzip)

    @mock_settings_for_test({"spark.write.stream.default_format": "json"})
    def test_write_with_default_output_format(self):
        karadoc.cli.run_command("stream --tables test_schema.test_table " "--streaming-mode once")
        output_files = os.listdir(f"{warehouse_dir}/test_schema.db/test_table/")
        is_gzip = any([x.endswith("json") for x in output_files])
        self.assertTrue(is_gzip)

    def test_run_without_write_options(self):
        karadoc.cli.run_command("stream --tables test_schema.without_write_options " "--streaming-mode once")
        output_files = os.listdir(f"{warehouse_dir}/test_schema.db/without_write_options/")
        is_gzip = any([x.endswith("gz") for x in output_files])
        self.assertFalse(is_gzip)

    def test_run_with_static_and_dynamic_partition_invalid(self):
        with self.assertRaises(Exception) as cm:
            karadoc.cli.run_command(
                "stream --vars day=2018-01-01 --tables test_schema.static_and_dynamic_partition_invalid "
                "--streaming-mode once"
            )
        the_exception = cm.exception
        self.assertIn("static partition defined after a dynamic partition", str(the_exception))

    @mock_settings_for_test({"spark.conf.spark.sql.streaming.schemaInference": "true"})
    def test_stream_inputs(self):
        karadoc.cli.run_command("stream --tables test_schema.test_table --streaming-mode once")

        def inspect_df(df: DataFrame):
            df_processed = stream_to_batch(df)
            self.assertEqual(MockDataFrame([MockRow(value="this is a test DataFrame")]), df_processed)

        with mock.patch("karadoc.cli.commands.stream.inspect_df", side_effect=inspect_df) as mock_inspect_df:
            karadoc.cli.run_command("stream --tables test_schema.test_table_inputs --streaming-mode once")
        mock_inspect_df.assert_called_once()
