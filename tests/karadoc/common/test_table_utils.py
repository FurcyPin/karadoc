from unittest import TestCase

from karadoc.common import table_utils


class TestTableUtils(TestCase):
    def test_parse_partition_name(self):
        self.assertEqual(table_utils.parse_partition_name(None), None)
        self.assertEqual(
            table_utils.parse_partition_name("day=2018-01-01/country=fr"), [("day", "2018-01-01"), ("country", "fr")]
        )

    def test_parse_table_name(self):
        self.assertEqual(table_utils.parse_table_name("schema.table"), ("schema", "table", None))

    def test_parse_table_name_with_partition(self):
        actual = table_utils.parse_table_name("schema.table/day=2018-01-01")
        expected = ("schema", "table", [("day", "2018-01-01")])
        self.assertEqual(actual, expected)

    def test_remove_partition_from_name(self):
        actual = table_utils.remove_partition_from_name("schema.table/day=2018-01-01")
        expected = "schema.table"
        self.assertEqual(actual, expected)
