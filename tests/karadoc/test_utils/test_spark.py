import unittest

from pyspark import Row

from karadoc.test_utils.assertion import Anything
from karadoc.test_utils.pyspark_test_class import PySparkTest
from karadoc.test_utils.spark import MockDataFrame, MockRow


class TestMockRow(unittest.TestCase):
    def test_mockrow_none_comparison(self):
        n = None
        self.assertIsNotNone(MockRow(a=[1, 2, 3]) == n)
        self.assertFalse(MockRow(a=[1, 2, 3]) == n)

        self.assertIsNotNone(MockRow(a=[1, 2, 3]) is None)
        self.assertFalse(MockRow(a=[1, 2, 3]) is None)

    def test_row_mockrow_comparison(self):
        self.assertEqual(Row(a=[1, 2, 3]), MockRow(a=[1, 2, 3]))
        self.assertNotEqual(Row(a=[3, 2, 1]), MockRow(a=[1, 2, 3]))
        self.assertEqual(Row(a=[1, 2, 3]), MockRow(a={1, 2, 3}))
        self.assertEqual(Row(a=[3, 2, 1]), MockRow(a={1, 2, 3}))

        self.assertNotEqual(Row(a=[3, 2]), MockRow(a={1, 2, 3}))
        self.assertNotEqual(Row(a=[3, 2, 1]), MockRow(a={2, 3}))
        self.assertNotEqual(Row(a=[1, 2, 3]), MockRow(a=[1, 2]))
        self.assertNotEqual(Row(a=[2, 3]), MockRow(a=[1, 2, 3]))

        self.assertNotEqual(Row(a=[2, 3]), MockRow(a=[2, 3], b=4))
        self.assertNotEqual(Row(a=[2, 3], b=4), MockRow(a=[2, 3]))

        self.assertNotEqual(Row(a=[2, 3]), MockRow(a=None))
        self.assertNotEqual(Row(a=None), MockRow(a=[2, 3]))

        self.assertEqual(Row(a=[2, 3]), MockRow(a=[2, Anything]))
        self.assertEqual(Row(a=[2, 3]), MockRow(a={2, Anything}))
        self.assertNotEqual(Row(a=[2, 3, 4]), MockRow(a=[2, Anything]))
        self.assertNotEqual(Row(a=[2, 3, 4]), MockRow(a={2, Anything}))


class TestMockDataFrame(PySparkTest):
    def test_nested_structures(self):
        actual = self.spark.createDataFrame([Row(e=1, d="2", c=[Row(b=3, a="4"), Row(b=5, a="6")])])
        expected = MockDataFrame([MockRow(c={MockRow(a="4", b=3), MockRow(a="6", b=5)}, d="2", e=1)])
        self.assertEqual(expected, actual)

        wrong_expected = MockDataFrame([MockRow(c=[MockRow(a="WRONG", b=3), MockRow(a="6", b=5)], d="2", e=1)])
        self.assertNotEqual(wrong_expected, actual)

    def test_duplicate_rows(self):
        actual = self.spark.createDataFrame([Row(a=1), Row(a=1)])
        expected = MockDataFrame([MockRow(a=1), MockRow(a=1)])
        self.assertEqual(expected, actual)

    def test_duplicate_rows_ignore_order(self):
        actual = self.spark.createDataFrame([Row(a=1), Row(a=1)])
        expected = MockDataFrame({MockRow(a=1), MockRow(a=1)})
        self.assertEqual(expected, actual)
