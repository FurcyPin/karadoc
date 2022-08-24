from unittest import TestCase

from karadoc.common.job_core import has_spark


class TestHasSpark(TestCase):
    def test_partition_to_path_suffix(self):
        actual = has_spark._partition_to_path(None)
        self.assertEqual(actual, "")

        actual = has_spark._partition_to_path([])
        self.assertEqual(actual, "")

        actual = has_spark._partition_to_path([("k1", "v1")])
        self.assertEqual(actual, "/k1=v1")

        actual = has_spark._partition_to_path([("k1", "v1"), ("k2", "v2")])
        self.assertEqual(actual, "/k1=v1/k2=v2")

        actual = has_spark._partition_to_path(["k0", ("k1", "v1"), "k2"])
        self.assertEqual(actual, "/k1=v1")
