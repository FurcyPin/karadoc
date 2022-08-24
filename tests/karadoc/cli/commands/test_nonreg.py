import shutil
import unittest
from unittest import mock

import karadoc
from karadoc.common.nonreg import NonregResult
from karadoc.test_utils.mock_settings import (
    mock_settings_for_test,
    mock_settings_for_test_class,
)
from karadoc.test_utils.spark import MockDataFrame, MockRow
from karadoc.test_utils.stdio import captured_output


@mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": "tests/resources/karadoc/cli/commands/test_nonreg_command/model",
        "warehouse_dir": "test_working_dir/hive/warehouse",
    }
)
class TestNonReg(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree("test_working_dir/hive", ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree("test_working_dir/hive", ignore_errors=True)

    def test_nonreg_ok(self):
        """Given a table test_schema.test_table
        When we perform a nonreg on it without modifying the model
        Then the nonreg_result is_ok attribute should be True
        """
        karadoc.cli.run_command("run --tables test_schema.table_from_var --vars var=a")

        def inspect_nonreg_results(nonreg_results: NonregResult):
            self.assertTrue(nonreg_results.is_ok)

        with captured_output() as (out, err), mock.patch(
            "karadoc.cli.commands.nonreg.inspect_nonreg_results", side_effect=inspect_nonreg_results
        ):
            karadoc.cli.run_command("nonreg --tables test_schema.table_from_var --vars var=a")
            self.assertIn("Row count: ok", out.getvalue())
            self.assertIn("Diff: ok", out.getvalue())

    def test_nonreg_not_ok(self):
        """Given a table test_schema.test_table
        When we perform a nonreg after modifying the model
        Then the nonreg_result is_ok attribute should be False
        """
        karadoc.cli.run_command("run --tables test_schema.table_from_var --vars var=a")

        def inspect_nonreg_results(nonreg_results: NonregResult):
            self.assertFalse(nonreg_results.is_ok)

        with captured_output() as (out, err), mock.patch(
            "karadoc.cli.commands.nonreg.inspect_nonreg_results", side_effect=inspect_nonreg_results
        ):
            karadoc.cli.run_command("nonreg --tables test_schema.table_from_var --vars var=b")
            self.assertIn("Row count: ok", out.getvalue())
            self.assertIn("Diff: not ok", out.getvalue())
            self.assertIn("1 rows disappeared", out.getvalue())
            self.assertIn("|`column name`|`total nb diff`|before|after|`nb differences`|", out.getvalue())
            self.assertIn("|var          |1              |a     |b    |1               |", out.getvalue())
            self.assertIn("Diff: not ok", out.getvalue())

    def test_nonreg_with_show_examples(self):
        """
        Given a table test_schema.test_table
        When we perform a nonreg after modifying the model with the --show-examples option
        Then the nonreg will print examples on the output
        """
        karadoc.cli.run_command("run --tables test_schema.table_from_var --vars var=a")

        def inspect_nonreg_results(nonreg_results: NonregResult):
            self.assertFalse(nonreg_results.is_ok)

        with captured_output() as (out, err), mock.patch(
            "karadoc.cli.commands.nonreg.inspect_nonreg_results", side_effect=inspect_nonreg_results
        ):
            karadoc.cli.run_command("nonreg --tables test_schema.table_from_var --vars var=b --show-examples")
            self.assertIn("Row count: ok", out.getvalue())
            self.assertIn("Diff: not ok", out.getvalue())
            self.assertIn("Detailed examples :", out.getvalue())
            self.assertIn("|id |before.var|after.var|", out.getvalue())
            self.assertIn("|1  |a         |b        |", out.getvalue())

    def test_nonreg_is_not_writing(self):
        """Given a table test_schema.test_table
        When run a nonreg after modifying the model
        Then table data shouldn't change
        """
        karadoc.cli.run_command("run --tables test_schema.table_from_var --vars var=a")
        karadoc.cli.run_command("nonreg --tables test_schema.table_from_var --vars var=b --join-cols id")

        from karadoc.common import Job

        job = Job()
        job.init()
        job.inputs = {"test_table": "test_schema.table_from_var"}
        df = job.read_table("test_table")
        self.assertEqual(MockDataFrame([MockRow(id=1, var="a")]), df)

    def test_nonreg_with_partition(self):
        """Given a partitioned table test_schema.partitioned_table
        When we perform a nonreg on a single partition
        Then nonreg should only be executed on that single partition
        """

        karadoc.cli.run_command("run --tables test_schema.partitioned_table --vars day=2018-02-01")
        karadoc.cli.run_command("run --tables test_schema.partitioned_table --vars day=2018-02-02")

        def inspect_nonreg_results(nonreg_results: NonregResult):
            self.assertTrue(nonreg_results.is_ok)

        with mock.patch("karadoc.cli.commands.nonreg.inspect_nonreg_results", side_effect=inspect_nonreg_results):
            karadoc.cli.run_command("nonreg --tables test_schema.partitioned_table --vars day=2018-02-02")

    def test_nonreg_compare_with_on_partitionned_table(self):
        """Given a partitioned table test_schema.partitioned_table
        When we perform a nonreg with compare-with argument on a single partition
        Then nonreg should only be executed on that single partition
        """
        karadoc.cli.run_command("run --tables test_schema.partitioned_table --vars day=2018-02-01")
        karadoc.cli.run_command("run --tables test_schema.partitioned_table --vars day=2018-02-02")

        with mock_settings_for_test({"warehouse_dir": "test_working_dir/hive/warehouse_2"}):
            karadoc.cli.run_command("run --tables test_schema.partitioned_table --vars day=2018-02-02")

        def inspect_nonreg_results(nonreg_results: NonregResult):
            self.assertTrue(nonreg_results.is_ok)

        test_env_conf = {
            "remote_env": {"remote": {"type": "default", "warehouse_dir": "test_working_dir/hive/warehouse_2"}}
        }

        with mock.patch(
            "karadoc.cli.commands.nonreg.inspect_nonreg_results", side_effect=inspect_nonreg_results
        ), mock_settings_for_test(test_env_conf):
            karadoc.cli.run_command(
                "nonreg --tables test_schema.partitioned_table --vars day=2018-02-02 " "--compare-with remote"
            )

    def test_nonreg_with_primary_key(self):
        """Given a table test_schema.test_table that has a primary key
        When we perform a nonreg on it without modifying the model
        Then the nonreg_result is_ok attribute should be True
        """
        karadoc.cli.run_command("run --tables test_schema.table_from_var --vars var=a")

        def inspect_nonreg_results(nonreg_results: NonregResult):
            self.assertFalse(nonreg_results.is_ok)

        with mock.patch("karadoc.cli.commands.nonreg.inspect_nonreg_results", side_effect=inspect_nonreg_results):
            karadoc.cli.run_command("nonreg --tables test_schema.table_from_var --vars var=b")
