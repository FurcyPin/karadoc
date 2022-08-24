import unittest
from typing import Dict
from unittest import mock

from pyspark.sql import DataFrame

import karadoc
from karadoc.test_utils.assertion import Anything
from karadoc.test_utils.mock_settings import mock_settings_for_test_class
from karadoc.test_utils.spark import MockDataFrame, MockRow


@mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": "tests/resources/karadoc/cli/commands/test_quality_check/model",
        "warehouse_dir": "test_working_dir/hive/warehouse",
        "connection": {
            "dummy": {
                "type": "tests.resources.connectors.dummy",
                "url": "my_url",
            }
        },
    }
)
class TestQualityCheck(unittest.TestCase):
    result_df: DataFrame = None

    def write_mock(self, df: DataFrame, dest: Dict):
        if self.result_df is None:
            self.result_df = df
        else:
            self.result_df = self.result_df.union(df)

    def setup(self):
        self.result_df = None

    def test_dry_run(self):
        karadoc.cli.run_command("quality_check --dry --tables test_schema.test_table")

    def test_run(self):
        karadoc.cli.run_command("quality_check --tables test_schema.test_table")

    def test_run_with_output_alert_ok(self):
        with mock.patch("tests.resources.connectors.dummy.DummyConnector.write", side_effect=self.write_mock):
            karadoc.cli.run_command(
                "quality_check --output-alert-table test_schema.output_table --tables test_schema.alert_ok"
            )

        expected = MockDataFrame(
            [
                MockRow(
                    alert=MockRow(
                        table="test_schema.alert_ok",
                        name="alert_ok",
                        description="This is a dummy ok alert",
                        severity="Debug",
                        evaluation_date=Anything,
                        status="ok",
                        rank=Anything,
                    ),
                    columns=[],
                )
            ]
        )
        self.assertEqual(expected, self.result_df)

    def test_run_with_output_alert_ko(self):
        with mock.patch("tests.resources.connectors.dummy.DummyConnector.write", side_effect=self.write_mock):
            karadoc.cli.run_command(
                "quality_check --output-alert-table test_schema.output_table --tables test_schema.alert_ko"
            )

        expected = MockDataFrame(
            [
                MockRow(
                    alert=MockRow(
                        table="test_schema.alert_ko",
                        name="alert_ko",
                        description="This is a dummy ko alert",
                        severity="Debug",
                        evaluation_date=Anything,
                        status="ko",
                        rank=Anything,
                    ),
                    columns={
                        MockRow(key="id", value="1234"),
                        MockRow(key="error.error", value="this is a dummy error"),
                    },
                )
            ]
        )
        self.assertEqual(expected, self.result_df)

    def test_run_with_output_alert_error(self):
        with mock.patch("tests.resources.connectors.dummy.DummyConnector.write", side_effect=self.write_mock):
            karadoc.cli.run_command(
                "quality_check --output-alert-table test_schema.output_table --tables test_schema.alert_error"
            )

        expected = MockDataFrame(
            [
                MockRow(
                    alert=MockRow(
                        table="test_schema.alert_error",
                        name="alert_error",
                        description="This is a dummy error alert",
                        severity="Debug",
                        evaluation_date=Anything,
                        status="error",
                        rank=Anything,
                    ),
                    columns={
                        MockRow(key="exception", value=Anything),
                    },
                )
            ]
        )
        self.assertEqual(expected, self.result_df)

    def test_run_with_output_alert_no_return(self):
        with mock.patch("tests.resources.connectors.dummy.DummyConnector.write", side_effect=self.write_mock):
            karadoc.cli.run_command(
                "quality_check --output-alert-table test_schema.output_table --tables test_schema.alert_no_return"
            )

        expected = MockDataFrame(
            [
                MockRow(
                    alert=MockRow(
                        table="test_schema.alert_no_return",
                        name="alert_no_return",
                        description="This is a dummy incorrect alert that returns nothing",
                        severity="Debug",
                        evaluation_date=Anything,
                        status="error",
                        rank=Anything,
                    ),
                    columns={
                        MockRow(key="exception", value=Anything),
                    },
                )
            ]
        )
        self.assertEqual(expected, self.result_df)

    def test_run_multiple_alerts(self):
        with mock.patch("tests.resources.connectors.dummy.DummyConnector.write", side_effect=self.write_mock):
            karadoc.cli.run_command(
                "quality_check --output-alert-table test_schema.output_table --tables test_schema.multiple_checks"
            )

        expected = MockDataFrame(
            {
                MockRow(
                    alert=MockRow(
                        table="test_schema.multiple_checks",
                        name="test_alert_1",
                        description="description of test_alert_1",
                        severity="Debug",
                        evaluation_date=Anything,
                        status="ok",
                        rank=Anything,
                    ),
                    columns=[],
                ),
                MockRow(
                    alert=MockRow(
                        table="test_schema.multiple_checks",
                        name="test_alert_2",
                        description="description of test_alert_2",
                        severity="Debug",
                        evaluation_date=Anything,
                        status="ok",
                        rank=Anything,
                    ),
                    columns=[],
                ),
            }
        )
        self.assertEqual(expected, self.result_df)

    def test_alert_gen(self):
        with mock.patch("tests.resources.connectors.dummy.DummyConnector.write", side_effect=self.write_mock):
            karadoc.cli.run_command(
                "quality_check --output-alert-table test_schema.output_table --tables test_schema.alert_gen"
            )

        expected = MockDataFrame(
            {
                MockRow(
                    alert=MockRow(
                        table="test_schema.alert_gen",
                        name="test_alert_1",
                        description="description of test_alert_1",
                        severity="Debug",
                        evaluation_date=Anything,
                        status="ok",
                        rank=Anything,
                    ),
                    columns=[],
                ),
                MockRow(
                    alert=MockRow(
                        table="test_schema.alert_gen",
                        name="test_alert_2",
                        description="description of test_alert_2",
                        severity="Debug",
                        evaluation_date=Anything,
                        status="ok",
                        rank=Anything,
                    ),
                    columns=[],
                ),
                MockRow(
                    alert=MockRow(
                        table="test_schema.alert_gen",
                        name="test_alert_3",
                        description="description of test_alert_3",
                        severity="Debug",
                        evaluation_date=Anything,
                        status="ok",
                        rank=Anything,
                    ),
                    columns=[],
                ),
            }
        )
        self.result_df.show(100, False)
        self.assertEqual(expected, self.result_df)

    def test_run_specific_alerts(self):
        with mock.patch("tests.resources.connectors.dummy.DummyConnector.write", side_effect=self.write_mock):
            karadoc.cli.run_command(
                "quality_check --output-alert-table test_schema.output_table "
                "--tables test_schema.multiple_checks --alerts test_alert_1"
            )

        expected = MockDataFrame(
            {
                MockRow(
                    alert=MockRow(
                        table="test_schema.multiple_checks",
                        name="test_alert_1",
                        description="description of test_alert_1",
                        severity="Debug",
                        evaluation_date=Anything,
                        status="ok",
                        rank=Anything,
                    ),
                    columns=[],
                )
            }
        )
        self.assertEqual(expected, self.result_df)

    def test_run_alerts_with_vars(self):
        with mock.patch("tests.resources.connectors.dummy.DummyConnector.write", side_effect=self.write_mock):
            karadoc.cli.run_command(
                "quality_check --output-alert-table test_schema.output_table "
                "--tables test_schema.alert_vars --vars day=2021-01-01"
            )

        expected = MockDataFrame(
            {
                MockRow(
                    alert=MockRow(
                        table="test_schema.alert_vars",
                        name="alert_ko",
                        description="This is a ko alert using vars",
                        severity="Debug",
                        evaluation_date=Anything,
                        status="ko",
                        rank=Anything,
                    ),
                    columns={
                        MockRow(key="id", value="1234"),
                        MockRow(key="day", value="2021-01-01"),
                    },
                )
            }
        )
        self.assertEqual(expected, self.result_df)
