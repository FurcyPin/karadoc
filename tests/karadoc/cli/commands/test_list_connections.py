import os
import shutil
import unittest
from pathlib import Path

import karadoc
from karadoc.test_utils.mock_settings import mock_settings_for_test_class

test_dir = "test_working_dir/list_connections"


@mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": "tests/resources/karadoc/cli/commands/test_list_connections/model",
        "connection": {
            "dummy": {
                "type": "tests.resources.connectors.dummy",
                "url": "my_url",
            },
            "dummy_disabled": {
                "type": "tests.resources.connectors.dummy",
                "disable": True,
                "url": "my_url",
            },
        },
    }
)
class TestListConnections(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree(test_dir, ignore_errors=True)
        os.makedirs(test_dir, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(test_dir, ignore_errors=True)

    def test_list_connections_with_markdown_output(self):
        karadoc.cli.run_command(f"list_connections --format markdown --output {test_dir}/connections.md")
        with open(f"{test_dir}/connections.md") as f:
            actual = f.read()
        expected = (
            "| full_table_name             | job_type       | conn_name      | conn_type                        | direction   | host   | username   | container   | database   | table   | job_disabled   | conn_disabled   |\n"  # noqa: E501
            "|:----------------------------|:---------------|:---------------|:---------------------------------|:------------|:-------|:-----------|:------------|:-----------|:--------|:---------------|:----------------|\n"  # noqa: E501
            "| test_schema.disabled        | SparkBatchJob  | dummy_disabled | tests.resources.connectors.dummy | input       |        |            |             |            |         | True           | True            |\n"  # noqa: E501
            "| test_schema.disabled        | SparkStreamJob | dummy_disabled | tests.resources.connectors.dummy | input       |        |            |             |            |         | True           | True            |\n"  # noqa: E501
            "| test_schema.external_input  | SparkBatchJob  | dummy          | tests.resources.connectors.dummy | input       |        |            |             |            |         | False          | False           |\n"  # noqa: E501
            "| test_schema.external_input  | SparkStreamJob | dummy          | tests.resources.connectors.dummy | input       |        |            |             |            |         | False          | False           |\n"  # noqa: E501
            "| test_schema.external_output | SparkBatchJob  | dummy          | tests.resources.connectors.dummy | output      |        |            |             |            |         | False          | False           |\n"  # noqa: E501
            "| test_schema.external_output | SparkBatchJob  | dummy          | tests.resources.connectors.dummy | output      |        |            |             |            |         | False          | False           |\n"  # noqa: E501
            "| test_schema.external_output | SparkStreamJob | dummy          | tests.resources.connectors.dummy | output      |        |            |             |            |         | False          | False           |"  # noqa: E501
        )
        print(actual)
        self.assertEqual(expected, actual)

    def test_list_connections_with_csv_output(self):
        karadoc.cli.run_command(f"list_connections --format csv --output {test_dir}/connections.csv")
        with open(f"{test_dir}/connections.csv") as f:
            actual = f.read()
        expected = (
            "full_table_name,job_type,conn_name,conn_type,direction,host,username,container,database,table,job_disabled,conn_disabled\n"  # noqa: E501
            "test_schema.disabled,SparkBatchJob,dummy_disabled,tests.resources.connectors.dummy,input,,,,,,True,True\n"  # noqa: E501
            "test_schema.disabled,SparkStreamJob,dummy_disabled,tests.resources.connectors.dummy,input,,,,,,True,True\n"  # noqa: E501
            "test_schema.external_input,SparkBatchJob,dummy,tests.resources.connectors.dummy,input,,,,,,False,False\n"  # noqa: E501
            "test_schema.external_input,SparkStreamJob,dummy,tests.resources.connectors.dummy,input,,,,,,False,False\n"  # noqa: E501
            "test_schema.external_output,SparkBatchJob,dummy,tests.resources.connectors.dummy,output,,,,,,False,False\n"  # noqa: E501
            "test_schema.external_output,SparkBatchJob,dummy,tests.resources.connectors.dummy,output,,,,,,False,False\n"  # noqa: E501
            "test_schema.external_output,SparkStreamJob,dummy,tests.resources.connectors.dummy,output,,,,,,False,False\n"  # noqa: E501
        )
        self.assertEqual(expected, actual)

    def test_list_connections_with_xlsx_output(self):
        karadoc.cli.run_command(f"list_connections --format xlsx --output {test_dir}/connections.xlsx")
        self.assertTrue(Path(f"{test_dir}/connections.xlsx").exists())
