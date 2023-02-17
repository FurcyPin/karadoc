import shutil
import unittest

import karadoc
from karadoc.common.commands.return_code import ReturnCode
from karadoc.test_utils.mock_settings import mock_settings_for_test_class
from karadoc.test_utils.spark import MockDataFrame, MockRow
from tests.karadoc.spark.utils import read_spark_table
from tests.karadoc.test_utils import get_resource_folder_path

DEBUG = False


@mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": get_resource_folder_path(__name__) + "/models",
        "warehouse_dir": "test_working_dir/hive/warehouse",
    }
)
class TestCheckKeys(unittest.TestCase):
    def _generic_test(self, table_name, expected_results: MockDataFrame):
        karadoc.cli.run_command(f"run --tables {table_name}")
        return_code = karadoc.cli.run_command(f"check_keys --tables {table_name} --output-alert quality_check.alerts")
        self.assertEqual(ReturnCode.Success, return_code)
        df = read_spark_table("quality_check.alerts").where("alert.status = 'ko'")
        if DEBUG:
            df.show(10, False)
            df.select("columns.key", "columns.value").show(10, False)
            df.select("columns").printSchema()
            expected_df = expected_results.to_dataframe()
            expected_df.show(10, False)
            expected_df.printSchema()
        self.assertEqual(df.select("columns"), expected_results)

    def setUp(self) -> None:
        shutil.rmtree("test_working_dir/hive", ignore_errors=True)

    def tearDown(self) -> None:
        shutil.rmtree("test_working_dir/hive", ignore_errors=True)

    def test_run_with_available_keys(self):
        table_name = "test_schema.table_with_keys"
        expected_results = MockDataFrame({MockRow(columns=[MockRow(key="null_keys", value="[c2]")])})
        self._generic_test(table_name, expected_results)

    def test_run_without_indicated_keys(self):
        karadoc.cli.run_command("run --tables test_schema.table_without_keys")
        with self.assertRaises(Exception) as cm:
            karadoc.cli.run_command("check_keys --tables test_schema.table_without_keys")
        the_exception = cm.exception
        self.assertEqual("ERROR: No key is defined for the job", str(the_exception))

    def test_run_with_duplicate_keys(self):
        table_name = "test_schema.table_with_duplicate_keys"
        expected_results = MockDataFrame(
            {
                MockRow(
                    columns={
                        MockRow(key="c3", value="2"),
                        MockRow(key="nb_duplicates", value="2"),
                    }
                ),
                MockRow(
                    columns={
                        MockRow(key="c1", value="1"),
                        MockRow(key="nb_duplicates", value="2"),
                    }
                ),
            }
        )
        self._generic_test(table_name, expected_results)

    def test_run_with_only_secondary_keys(self):
        table_name = "test_schema.table_only_with_secondary_keys"
        expected_results = MockDataFrame(
            {
                MockRow(columns=[MockRow(key="null_keys", value="[c2]")]),
                MockRow(
                    columns={
                        MockRow(key="c3", value="a"),
                        MockRow(key="nb_duplicates", value="3"),
                    }
                ),
            }
        )
        self._generic_test(table_name, expected_results)

    def test_run_with_key_in_struct(self):
        table_name = "test_schema.table_with_key_in_struct"
        expected_results = MockDataFrame({MockRow(columns=[MockRow(key="null_keys", value="[json1.a]")])})
        self._generic_test(table_name, expected_results)

    def test_null_primary_keys(self):
        table_name = "test_schema.table_with_null_primary_keys"
        expected_results = MockDataFrame(
            {
                MockRow(columns=[MockRow(key="null_keys", value="[c3]")]),
                MockRow(columns=[MockRow(key="null_keys", value="[c2]")]),
                MockRow(columns=[MockRow(key="null_keys", value="[c1, c2]")]),
                MockRow(columns=[MockRow(key="null_keys", value="[c1, c2, c3]")]),
                MockRow(columns=[MockRow(key="null_keys", value="[c1, c2, c3]")]),
                MockRow(columns=[MockRow(key="null_keys", value="[c1, c2, c3]")]),
                MockRow(
                    columns={
                        MockRow(key="c1", value=None),
                        MockRow(key="c2", value=None),
                        MockRow(key="c3", value=None),
                        MockRow(key="nb_duplicates", value="3"),
                    }
                ),
            }
        )

        self._generic_test(table_name, expected_results)
