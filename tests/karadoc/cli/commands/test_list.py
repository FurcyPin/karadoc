import os
import shutil
import unittest
from pathlib import Path
from unittest import mock

import karadoc
from karadoc.test_utils.mock_settings import mock_settings_for_test_class
from tests.karadoc.test_utils import get_resource_folder_path


@mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": get_resource_folder_path(__name__) + "/models",
        "warehouse_dir": "test_working_dir/hive/warehouse",
    }
)
class TestList(unittest.TestCase):
    @mock.patch("builtins.print")
    def test_list_tables_with_keys(self, mock_print):
        karadoc.cli.run_command("list --with-keys")
        mock_print.assert_any_call("test_schema.table_with_keys_1")
        mock_print.assert_any_call("test_schema.table_with_keys_2")
        self.assertEqual(mock_print.call_count, 2)

    @mock.patch("builtins.print")
    def test_list_tables_with_quality_check(self, mock_print):
        karadoc.cli.run_command("list --has-quality-check")
        mock_print.assert_any_call("test_schema.table_with_quality_check_1")
        mock_print.assert_any_call("test_schema.table_with_quality_check_2")
        self.assertEqual(mock_print.call_count, 2)

    @mock.patch("builtins.print")
    def test_list_tables_disabled(self, mock_print):
        karadoc.cli.run_command("list --is-disabled")
        mock_print.assert_any_call("test_schema.table_with_keys_1")
        self.assertEqual(mock_print.call_count, 1)

    @mock.patch("builtins.print")
    def test_list_tables_disabled_and_having_keys(self, mock_print):
        karadoc.cli.run_command("list --is-disabled --with-keys")
        mock_print.assert_any_call("test_schema.table_with_keys_1")
        self.assertEqual(mock_print.call_count, 1)

    def test_list_tables_with_output(self):
        path = "test_working_dir/generated_content"
        if Path(path).is_dir() is False:
            os.makedirs(path, exist_ok=True)
        karadoc.cli.run_command("list --with-keys --output test_working_dir/generated_content/text_tables_disabled.txt")
        with open("test_working_dir/generated_content/text_tables_disabled.txt") as m:
            self.assertEqual(m.read(), "test_schema.table_with_keys_1\ntest_schema.table_with_keys_2\n")
        shutil.rmtree("test_working_dir/generated_content")
