from karadoc.common.nonreg import DataframeComparator, NonregResult
from karadoc.test_utils import pyspark_test_class
from karadoc.test_utils.assertion import Anything


class TestNonReg(pyspark_test_class.PySparkTest):
    df_comparator = DataframeComparator()

    def test_compare_df_with_simplest(self):
        # fmt: off
        df = self.spark.createDataFrame(
            [(1, "a"),
             (2, "b"),
             (3, "c")],
            "id INT, name STRING"
        )
        # fmt: on
        self.assertEqual(self.df_comparator.compare_df(df, df), NonregResult(True, None))

    def test_compare_df_with_ordering(self):
        # fmt: off
        df_1 = self.spark.createDataFrame(
            [(1, "a"),
             (2, "b"),
             (3, "c")],
            "id INT, name STRING"
        )
        df_2 = self.spark.createDataFrame(
            [(2, "b"),
             (3, "c"),
             (1, "a")],
            "id INT, name STRING"
        )
        # fmt: on
        self.assertEqual(self.df_comparator.compare_df(df_1, df_2), NonregResult(True, None))

    def test_compare_df_with_annoying_column_names(self):
        # fmt: off
        df_1 = self.spark.createDataFrame([
            (1, "a"),
            (2, "b"),
            (3, "c")],
            "id INT, `annoying.column.name` STRING"
        )
        df_2 = self.spark.createDataFrame([
            (1, "a"),
            (2, "b"),
            (3, "d")],
            "id INT, `annoying.column.name` STRING"
        )
        # fmt: on
        self.assertEqual(self.df_comparator.compare_df(df_1, df_2), NonregResult(True, Anything))

    def test_compare_df_with_structs(self):
        # fmt: off
        df_1 = self.spark.createDataFrame(
            [(1, {"a": 1, "b": 2, "c": 3}),
             (2, {"a": 1, "b": 2, "c": 3}),
             (3, {"a": 1, "b": 2, "c": 3})],
            "id INT, a STRUCT<a: INT, b: INT, c: Int>",
        )
        df_2 = self.spark.createDataFrame(
            [(1, {"a": 1, "b": 2, "c": 3}),
             (2, {"a": 1, "b": 2, "c": 3}),
             (3, {"a": 1, "b": 2, "c": 4})],
            "id INT, a STRUCT<a: INT, b: INT, c: Int>",
        )
        # fmt: on
        self.assertEqual(self.df_comparator.compare_df(df_1, df_2), NonregResult(True, Anything))

    def test_compare_df_with_arrays(self):
        # fmt: off
        df_1 = self.spark.createDataFrame(
            [(1, [1, 2, 3]),
             (2, [1, 2, 3]),
             (3, [1, 2, 3])],
            "id INT, a ARRAY<INT>"
        )
        df_2 = self.spark.createDataFrame(
            [(2, [1, 2, 3]),
             (3, [3, 2, 1]),
             (1, [3, 1, 2])],
            "id INT, a ARRAY<INT>"
        )
        # fmt: on
        self.assertEqual(self.df_comparator.compare_df(df_1, df_2), NonregResult(True, None))

    def test_compare_df_with_maps(self):
        # fmt: off
        df_1 = self.spark.createDataFrame(
            [(1, {"a": 1, "b": 2, "c": 3}),
             (2, {"a": 1, "b": 2, "c": 3}),
             (3, {"a": 1, "b": 2, "c": 3})],
            "id INT, a MAP<STRING, INT>",
        )
        df_2 = self.spark.createDataFrame(
            [(2, {"a": 1, "b": 2, "c": 3}),
             (3, {"c": 3, "b": 2, "a": 1}),
             (1, {"c": 3, "a": 1, "b": 2})],
            "id INT, a MAP<STRING, INT>",
        )
        # fmt: on
        self.assertEqual(self.df_comparator.compare_df(df_1, df_2), NonregResult(True, None))

    def test_compare_df_with_differences(self):
        # fmt: off
        df_1 = self.spark.createDataFrame([
            (1, "a"),
            (2, "b"),
            (3, "c")],
            "id INT, name STRING"
        )
        df_2 = self.spark.createDataFrame([
            (1, "a"),
            (2, "b"),
            (3, "d")],
            "id INT, name STRING"
        )
        # fmt: on
        self.assertEqual(self.df_comparator.compare_df(df_1, df_2), NonregResult(True, Anything))

    def test_compare_df_with_differing_types(self):
        # fmt: off
        df_1 = self.spark.createDataFrame([
            (1, "a"),
            (2, "b"),
            (3, "c")],
            "id INT, name STRING"
        )
        df_2 = self.spark.createDataFrame([
            (1, "a"),
            (2, "b"),
            (3, "c")],
            "id BIGINT, name STRING"
        )
        # fmt: on
        self.assertEqual(self.df_comparator.compare_df(df_1, df_2), NonregResult(False, Anything))
