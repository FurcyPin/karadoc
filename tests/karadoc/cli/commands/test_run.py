import os
import shutil
import unittest
from pathlib import Path
from unittest import mock

import karadoc
from karadoc.common.exceptions import ActionFileLoadingError
from karadoc.test_utils.mock_settings import mock_settings_for_test, mock_settings_for_test_class
from karadoc.test_utils.stdio import captured_output
from tests.karadoc.test_utils import get_resource_folder_path

warehouse_dir = "test_working_dir/test_run_warehouse"


@mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": get_resource_folder_path(__name__) + "/models",
        "warehouse_dir": warehouse_dir,
    }
)
class TestRun(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree(warehouse_dir, ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree(warehouse_dir, ignore_errors=True)

    def test_dry_run(self):
        karadoc.cli.run_command("run --dry --models test_schema.test_table")

    def test_run(self):
        karadoc.cli.run_command("run --models test_schema.test_table")

    def test_run_with_limit_output(self):
        def inspect_df(df):
            self.assertEqual(df.count(), 2)

        with mock.patch("karadoc.cli.commands.run.inspect_df", side_effect=inspect_df) as check_mock:
            karadoc.cli.run_command("run --models test_schema.limit_output")
        check_mock.assert_called_once()

        def inspect_df(df):
            self.assertEqual(df.count(), 1)

        with mock.patch("karadoc.cli.commands.run.inspect_df", side_effect=inspect_df) as check_mock:
            karadoc.cli.run_command("run --limit-output 1 --models test_schema.limit_output")
        check_mock.assert_called_once()

    def test_run_with_limit_inputs(self):
        """When using the option --limit-inputs, the number of rows of the inputs read by a job should be limited"""

        def inspect_df(df):
            self.assertEqual(df.count(), 2)

        with mock.patch("karadoc.cli.commands.run.inspect_df", side_effect=inspect_df) as check_mock:
            karadoc.cli.run_command("run --models test_schema.input_table")
        check_mock.assert_called_once()

        def inspect_df(df):
            self.assertEqual(df.count(), 2)

        with mock.patch("karadoc.cli.commands.run.inspect_df", side_effect=inspect_df) as check_mock:
            karadoc.cli.run_command("run --models test_schema.limit_inputs")
        check_mock.assert_called_once()

        def inspect_df(df):
            self.assertEqual(df.count(), 1)

        with mock.patch("karadoc.cli.commands.run.inspect_df", side_effect=inspect_df) as check_mock:
            karadoc.cli.run_command("run --limit-inputs 1 --models test_schema.limit_inputs")
        check_mock.assert_called_once()

    def test_run_with_limit_inputs_with_load_inputs_as_view(self):
        """When using the option --limit-inputs, the number of rows of the inputs read by a job should be limited,
        even when using the load_inputs_as_view method"""

        def inspect_df(df):
            self.assertEqual(df.count(), 2)

        with mock.patch("karadoc.cli.commands.run.inspect_df", side_effect=inspect_df) as check_mock:
            karadoc.cli.run_command("run --models test_schema.input_table")
        check_mock.assert_called_once()

        def inspect_df(df):
            self.assertEqual(df.count(), 2)

        with mock.patch("karadoc.cli.commands.run.inspect_df", side_effect=inspect_df) as check_mock:
            karadoc.cli.run_command("run --models test_schema.limit_inputs_with_load_inputs_as_view")
        check_mock.assert_called_once()

        def inspect_df(df):
            self.assertEqual(df.count(), 1)

        with mock.patch("karadoc.cli.commands.run.inspect_df", side_effect=inspect_df) as check_mock:
            karadoc.cli.run_command("run --limit-inputs 1 --models test_schema.limit_inputs_with_load_inputs_as_view")
        check_mock.assert_called_once()

    def test_run_with_limit_output_return_none(self):
        """The --limit-output option should not crash when no DataFrame is returned"""
        karadoc.cli.run_command("run --limit-output 2 --models test_schema.limit_output_return_none")

    def test_run_with_limit_output_return_empty(self):
        """The --limit-output option should not crash when the return DataFrame is already empty"""
        karadoc.cli.run_command("run --limit-output 2 --models test_schema.limit_output_return_empty")

    def test_run_with_partitions(self):
        karadoc.cli.run_command("run --vars day=2018-02-02 --models test_schema.partition_table")
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/partition_table/day=2018-02-02").is_dir())

    @mock_settings_for_test({"spark.conf": {"spark.sql.sources.partitionOverwriteMode": "DYNAMIC"}})
    def test_overwrite_spark_conf_in_job(self):
        """
        Given a POPULATE that reads from a partitionned table
        When we set in the run method spark.sql.sources.partitionOverwriteMode to STATIC
        Then it should overwrites the target partitions and delete all the other partitions
        """
        karadoc.cli.run_command("run --vars day=2018-02-01 --models test_schema.partition_table")
        karadoc.cli.run_command("run --vars day=2018-02-02 --models test_schema.partition_table")
        karadoc.cli.run_command("run --models test_schema.test_table_spark_conf_overwrite")
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/test_table_spark_conf_overwrite/day=2018-02-01").is_dir())
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/test_table_spark_conf_overwrite/day=2018-02-02").is_dir())
        shutil.rmtree(f"{warehouse_dir}/test_schema.db/partition_table", ignore_errors=True)
        karadoc.cli.run_command("run --vars day=2018-02-01 --models test_schema.partition_table")
        karadoc.cli.run_command("run --models test_schema.test_table_spark_conf_overwrite")
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/test_table_spark_conf_overwrite/day=2018-02-01").is_dir())
        self.assertFalse(
            Path(f"{warehouse_dir}/test_schema.db/test_table_spark_conf_overwrite/day=2018-02-02").is_dir()
        )

    @mock_settings_for_test({"libs": get_resource_folder_path(__name__) + "/libs"})
    def test_run_with_populate_with_udf_in_libs_folder(self):
        """
        GIVEN a POPULATE that uses a UDF that uses another function
        WHEN we run it
        THEN no import error should happen
        The error that usually happens is "ModuleNotFoundError: No module named 'udfs' at pyspark/serializers.py"
        """
        # This simulates the first time the karadoc module is imported
        from karadoc.common.conf.package import _add_libs_folder_to_python_path  # noqa: E402

        _add_libs_folder_to_python_path()

        karadoc.cli.run_command("run --models test_schema.udf_table")
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/udf_table").is_dir())

    def test_run_with_partition_range(self):
        karadoc.cli.run_command(
            'run --vars day=day_range("2018-02-02","2018-02-05") --models test_schema.partition_table'
        )
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/partition_table/day=2018-02-02").is_dir())
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/partition_table/day=2018-02-03").is_dir())
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/partition_table/day=2018-02-04").is_dir())
        self.assertFalse(Path(f"{warehouse_dir}/test_schema.db/partition_table/day=2018-02-05").is_dir())

    def test_run_with_dynamic_partitions(self):
        karadoc.cli.run_command("run --models test_schema.dynamic_partition_only")
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/dynamic_partition_only/day=2018-01-01/BU=US").is_dir())
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/dynamic_partition_only/day=2018-01-01/BU=FR").is_dir())
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/dynamic_partition_only/day=2018-01-02/BU=US").is_dir())
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/dynamic_partition_only/day=2018-01-02/BU=FR").is_dir())
        self.assertTrue(Path(f"{warehouse_dir}/test_schema.db/dynamic_partition_only/day=2018-01-03/BU=US").is_dir())

    def test_run_with_static_and_dynamic_partitions(self):
        karadoc.cli.run_command("run --vars day=2018-01-01 --models test_schema.static_and_dynamic_partition")
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
        karadoc.cli.run_command("run --models test_schema.with_write_options")
        output_files = os.listdir(f"{warehouse_dir}/test_schema.db/with_write_options/")
        is_gzip = any([x.endswith("gz") for x in output_files])
        self.assertTrue(is_gzip)

    @mock_settings_for_test({"spark.write.options.json": {"compression": "gzip"}})
    def test_run_write_options_in_settings(self):
        karadoc.cli.run_command("run --models test_schema.without_write_options")
        output_files = os.listdir(f"{warehouse_dir}/test_schema.db/without_write_options/")
        is_gzip = any([x.endswith("gz") for x in output_files])
        self.assertTrue(is_gzip)

    @mock_settings_for_test({"spark.write.options.json": {"compression": "gzip"}})
    def test_run_write_options_in_settings_and_populate(self):
        karadoc.cli.run_command("run --models test_schema.with_write_options")
        output_files = os.listdir(f"{warehouse_dir}/test_schema.db/with_write_options/")
        is_gzip = any([x.endswith("gz") for x in output_files])
        self.assertTrue(is_gzip)

    def test_run_without_write_options(self):
        karadoc.cli.run_command("run --models test_schema.without_write_options")
        output_files = os.listdir(f"{warehouse_dir}/test_schema.db/without_write_options/")
        is_gzip = any([x.endswith("gz") for x in output_files])
        self.assertFalse(is_gzip)

    def test_run_with_static_and_dynamic_partition_invalid(self):
        with self.assertRaises(Exception) as cm:
            karadoc.cli.run_command(
                "run --vars day=2018-01-01 --models test_schema.static_and_dynamic_partition_invalid"
            )
        the_exception = cm.exception
        self.assertIn("static partition defined after a dynamic partition", str(the_exception))

    def test_run_with_job_init_invalid(self):
        """
        Given a a POPULATE that calls job.init()
        When it is run
        Then an exception should be raised
        """
        with self.assertRaises(ActionFileLoadingError) as cm:
            karadoc.cli.run_command("run --models test_schema.job_init_invalid")
        the_exception = cm.exception
        self.assertIn("Could not load POPULATE.py file for table test_schema.job_init_invalid", str(the_exception))
        self.assertEqual(
            "Error: the spark context should not be initialized in an action file.", str(the_exception.__cause__)
        )

    def test_run_disabled_job(self):
        """
        Given a POPULATE.py with a disabled job
        When we run it
        Then an exception should be raised
        """
        with self.assertRaises(Exception) as cm:
            karadoc.cli.run_command("run --models test_schema.disabled_job")
        the_exception = cm.exception
        self.assertEqual("The job has been disabled, it should not be launched", str(the_exception))

    def test_dry_run_disabled_job(self):
        """
        Given a POPULATE.py with a disabled job
        When we run it in dry mode
        A warning should be raised
        """
        with captured_output() as (out, err):
            karadoc.cli.run_command("run --dry --models test_schema.disabled_job")
        self.assertIn("WARN: The job has been disabled, it should not be launched", out.getvalue())
