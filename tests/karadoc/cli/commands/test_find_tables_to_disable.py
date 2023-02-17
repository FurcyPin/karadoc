import os
import shutil
import unittest

import karadoc
from karadoc.test_utils.mock_settings import mock_settings_for_test, mock_settings_for_test_class
from karadoc.test_utils.strings import strip_margin
from tests.karadoc.test_utils import get_resource_folder_path

test_dir = "test_working_dir/find_tables_to_disable"


@mock_settings_for_test_class()
class TestFindTablesToDisable(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree(test_dir, ignore_errors=True)
        os.makedirs(test_dir, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(test_dir, ignore_errors=True)

    @mock_settings_for_test(
        {
            "enable_file_index_cache": False,
            "model_dir": get_resource_folder_path(__name__) + "/models",
        }
    )
    def test_find_tables_to_disable(self):
        karadoc.cli.run_command(f"find_tables_to_disable --format markdown --output {test_dir}/connections.md")
        with open(f"{test_dir}/connections.md") as f:
            actual = f.read()
        expected = strip_margin(
            """
            || full_table_name   |
            ||:------------------|
            || dead.D1           |
            || dead.D2           |"""
        )
        self.assertEqual(expected, actual)

    @mock_settings_for_test(
        {
            "enable_file_index_cache": False,
            "model_dir": get_resource_folder_path(__name__) + "/models_single_table",
        }
    )
    def test_find_tables_to_disable_single_table(self):
        karadoc.cli.run_command(f"find_tables_to_disable --format markdown --output {test_dir}/connections.md")
        with open(f"{test_dir}/connections.md") as f:
            actual = f.read()
        expected = strip_margin(
            """
            || full_table_name   |
            ||:------------------|
            || dead.D1           |"""
        )
        self.assertEqual(expected, actual)

    @mock_settings_for_test(
        {
            "enable_file_index_cache": False,
            "model_dir": get_resource_folder_path(__name__) + "/models",
        }
    )
    def show_graph_for_test_case(self):
        """Method use to visualize the test case. Rename this method by prefixing it with 'test' then run it
        to visualize the graph of the corresponding test case"""
        karadoc.cli.run_command("show_graph -a -b --tables dead.D3 alive.A3")
