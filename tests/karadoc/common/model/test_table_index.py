import unittest

from karadoc.common.model import table_index
from karadoc.test_utils.mock_settings import mock_settings_for_test_class


@mock_settings_for_test_class(
    {"enable_file_index_cache": False, "model_dir": "tests/resources/karadoc/common/model/test_table_index/model"}
)
class TableIndex(unittest.TestCase):
    def test_build_table_index(self):
        index = table_index.build_table_index()
        expected_keys = {
            "partitions.static_partition",
            "partitions.no_partition",
            "partitions.dynamic_partition",
            "partitions.variable_partition",
        }
        self.assertSetEqual(set(index.keys()), expected_keys)
        static_partition = index["partitions.static_partition"].populate
        dynamic_partition = index["partitions.dynamic_partition"].populate
        no_partition = index["partitions.no_partition"].populate
        variable_partition = index["partitions.variable_partition"].populate

        self.assertListEqual(static_partition.partitions, ["day"])
        self.assertListEqual(dynamic_partition.partitions, ["day"])
        self.assertListEqual(no_partition.partitions, [])
        self.assertListEqual(variable_partition.partitions, ["day"])

        self.assertDictEqual(variable_partition.vars, {"day": "2020-01-01"})
