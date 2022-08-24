import difflib
from typing import Dict, List, Optional, Tuple

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as f
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from pyspark.storagelevel import StorageLevel

from karadoc.common import spark_utils
from karadoc.common.spark_utils import quote, quote_columns


class NonregResult:
    def __init__(self, same_schema: bool, diff_df: Optional[DataFrame]):
        self.same_schema = same_schema
        self.diff_df = diff_df

    @property
    def same_data(self):
        return self.diff_df is None

    @property
    def is_ok(self):
        return self.same_schema and self.same_data

    def __eq__(self, other):
        if isinstance(other, NonregResult):
            return self.same_schema == other.same_schema and self.diff_df == other.diff_df
        else:
            return NotImplemented

    def __repr__(self):
        return f"NonregResult(same_schema={self.same_schema}, diff_df={self.diff_df})"


class DataframeComparator:
    DEFAULT_NB_DIFFED_ROWS = 10
    DEFAULT_MAX_STRING_LENGTH = 30
    DEFAULT_LEFT_DF_ALIAS = "left"
    DEFAULT_RIGHT_DF_ALIAS = "right"

    def __init__(
        self,
        nb_diffed_rows: int = DEFAULT_NB_DIFFED_ROWS,
        max_string_length: int = DEFAULT_MAX_STRING_LENGTH,
        left_df_alias: str = DEFAULT_LEFT_DF_ALIAS,
        right_df_alias: str = DEFAULT_RIGHT_DF_ALIAS,
    ):
        self.nb_diffed_rows = nb_diffed_rows
        self.left_df_alias = left_df_alias
        self.right_df_alias = right_df_alias
        self.max_string_length = max_string_length

    @staticmethod
    def _get_number_of_duplicates(df: DataFrame, col: str):
        """Counts the number of duplicate rows for the given column in the given DataFrame

        Example: If a column has one value present on 2 rows and another value present on 3 rows, the total number of
        duplicate is 5. If a column unique on each row, it's number of duplicates will be 0.

        >>> spark = spark_utils.get_spark_session("doctest")
        >>> df = spark.createDataFrame(
        ...     [(1,"a"),(2,"b"),(3,"b"),(4,"c"),(5,"c"),(6,"c"),],
        ...     "`id.col` INT, `name.col` STRING")
        >>> DataframeComparator._get_number_of_duplicates(df, "id.col")
        0
        >>> DataframeComparator._get_number_of_duplicates(df, "name.col")
        5

        :param df: a Spark DataFrame
        :param col: a column name
        :return: number of duplicate rows
        """
        return (
            df.select(quote(col))
            .groupBy(quote(col))
            .count()
            .where("count > 1")
            .agg(f.coalesce(f.sum("count"), f.lit(0)).alias("nb_duplicates"))
            .take(1)[0]
            .nb_duplicates
        )

    @staticmethod
    def _get_eligible_columns_for_join(df: DataFrame) -> Dict[str, int]:
        """Identifies the column with the least duplicates, in order to use it as the id for the comparison join.

        Eligible columns are all columns of type String, Int or Bigint that have an approximate distinct count of 90%
        of the number of rows in the DataFrame. Returns None if no such column is found.

        >>> spark = spark_utils.get_spark_session("doctest")
        >>> df = spark.createDataFrame(
        ...     [(1,"a"),(2,"b"),(3,"b"),(4,"c"),(5,"c"),(6,"c"),]
        ...     , "`id.col` INT, `name.col` STRING")
        >>> DataframeComparator._get_eligible_columns_for_join(df)
        {'id.col': 0}
        >>> df = spark.createDataFrame([(1,"a"),(1,"a")], "`id.col` INT, `name.col` STRING")
        >>> DataframeComparator._get_eligible_columns_for_join(df)
        {}

        :param df: a Spark DataFrame
        :return: The name of the columns that have more than 90% distinct values, and their corresponding number of
            of duplicates
        """
        eligible_cols = [
            col.name for col in df.schema.fields if type(col.dataType) in [StringType, IntegerType, LongType]
        ]

        distinct_count_threshold = f.lit(90.0)
        eligibility_df = df.select(
            [
                (
                    f.approx_count_distinct(quote(col)) * f.lit(100.0) / f.count(f.lit(1)) > distinct_count_threshold
                ).alias(col)
                for col in eligible_cols
            ]
        )
        columns_with_high_distinct_count = [key for key, value in eligibility_df.collect()[0].asDict().items() if value]
        cols_with_duplicates = {
            col: DataframeComparator._get_number_of_duplicates(df, col) for col in columns_with_high_distinct_count
        }
        return cols_with_duplicates

    @staticmethod
    def _merge_count_dicts(left_dict: Dict[str, int], right_dict: Dict[str, int]):
        """

        >>> DataframeComparator._merge_count_dicts({"a": 1, "b": 1}, {"a": 1, "c": 1})
        {'a': 2, 'b': 1, 'c': 1}

        :param left_dict:
        :param right_dict:
        :return:
        """
        res = left_dict.copy()
        for x in right_dict:
            if x in left_dict:
                res[x] += right_dict[x]
            else:
                res[x] = right_dict[x]
        return res

    @staticmethod
    def _automatically_infer_join_col(left_df: DataFrame, right_df: DataFrame) -> Optional[str]:
        """Identifies the column with the least duplicates, in order to use it as the id for the comparison join.

        Eligible columns are all columns of type String, Int or Bigint that have an approximate distinct count of 90%
        of the number of rows in the DataFrame. Returns None if no suche column is found.

        >>> spark = spark_utils.get_spark_session("doctest")
        >>> left_df = spark.createDataFrame(
        ...     [(1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"e"),(6,"f")],
        ...     "`id.col` INT, `name.col` STRING")
        >>> right_df = spark.createDataFrame(
        ...     [(1,"a"),(2,"a"),(3,"b"),(4,"c"),(5,"d"),(6,"e")],
        ...     "`id.col` INT, `name.col` STRING")
        >>> DataframeComparator._automatically_infer_join_col(left_df, right_df)
        'id.col'
        >>> left_df = spark.createDataFrame([(1,"a"),(1,"a")], "`id.col` INT, `name.col` STRING")
        >>> right_df = spark.createDataFrame([(1,"a"),(1,"a")], "`id.col` INT, `name.col` STRING")
        >>> DataframeComparator._automatically_infer_join_col(left_df, right_df)


        :param left_df: a Spark DataFrame
        :param right_df: a Spark DataFrame
        :return: The name of the column with the least duplicates in both DataFrames if it has less than 10% duplicates.
        """
        left_col_dict = DataframeComparator._get_eligible_columns_for_join(left_df)
        right_col_dict = DataframeComparator._get_eligible_columns_for_join(left_df)
        merged_col_dict = DataframeComparator._merge_count_dicts(left_col_dict, right_col_dict)

        if len(merged_col_dict) > 0:
            col, nb_duplicates = sorted(merged_col_dict.items(), key=lambda x: x[1])[0]
            return col
        else:
            return None

    @staticmethod
    def _canonize_col(field: StructField, col: Column = None) -> List[Column]:
        """Applies a transformation on the specified field depending on its type.
        This ensures that the hash of the column will be constant despite it's ordering.

        :param field: the StructField object
        :param col: optional: the Column object to canonize.
        If not given we will take the column corresponding to the fields name.
        Passing this parameter is useful to refer to a column from a specific DataFrame, for instance after a join.
        :return: a list of Columns
        """
        if col is None:
            col = f.col("`" + field.name + "`")
        if type(field.dataType) == ArrayType:
            return [f.sort_array(col)]
        elif type(field.dataType) == MapType:
            return [f.sort_array(f.map_keys(col)), f.sort_array(f.map_values(col))]
        else:
            return [col]

    @staticmethod
    def _schema_to_string(
        schema: StructType, include_nullable: bool = False, include_metadata: bool = False
    ) -> List[str]:
        res = []
        for field in schema:
            s = f"{field.name} {field.dataType.simpleString().upper()}"
            if include_nullable:
                if field.nullable:
                    s += " (nullable)"
                else:
                    s += " (not nullable)"
            if include_metadata:
                s += f" {field.metadata}"
            res.append(s)
        return res

    @staticmethod
    def _compare_schemas(left_df, right_df) -> bool:
        """Compares two DataFrames schemas and print out the differences.
        Ignore the nullable and comment attributes.

        Example:

        >>> spark = spark_utils.get_spark_session("doctest")
        >>> left_df = spark.createDataFrame([], "id INT, c1 STRING, c2 STRING, c4 ARRAY<STRUCT<a: INT, b: STRING>>")
        >>> right_df = spark.createDataFrame([], "id INT, c1 INT, c3 STRING, c4 ARRAY<STRUCT<a: INT, d: STRING>>")
        >>> res = DataframeComparator._compare_schemas(left_df, right_df)
        Schema has changed:
        @@ -1,5 +1,5 @@
        <BLANKLINE>
         id INT
        -c1 STRING
        -c2 STRING
        +c1 INT
        +c3 STRING
         c4!.a INT
        -c4!.b STRING
        +c4!.d STRING
        >>> res
        False

        :param left_df: a DataFrame
        :param right_df: another DataFrame
        :return: True if both DataFrames have the same schema, False otherwise.
        """
        left_flat_schema = spark_utils.flatten_schema(left_df.schema, explode=True)
        right_flat_schema = spark_utils.flatten_schema(right_df.schema, explode=True)

        left_string_schema = DataframeComparator._schema_to_string(left_flat_schema)
        right_string_schema = DataframeComparator._schema_to_string(right_flat_schema)

        diff = list(difflib.unified_diff(left_string_schema, right_string_schema))[2:]

        if len(diff) > 0:
            diff_string = "\n".join(diff)
            print(f"Schema has changed:\n{diff_string}")
            return False
        else:
            print(f"Schema: ok ({len(left_df.columns)} columns)")
            return True

    def _compare_rowcounts(self, left_df, right_df):
        """Compares the number of columns between two DataFrames and print out the differences

        :param left_df: a DataFrame
        :param right_df: another DataFrame
        :return: True if both DataFrames have the same number of rows, False otherwise.
        """
        left_count = left_df.count()
        right_count = right_df.count()
        diff_count = right_count - left_count
        if diff_count != 0:
            diff_string = "more" if diff_count > 0 else "less"
            same_rowcount = False
            print(
                f"Row count has changed: \n"
                f" {self.left_df_alias}: {left_count}\n"
                f" {self.right_df_alias}: {right_count} ({abs(diff_count)} {diff_string})"
            )
        else:
            same_rowcount = True
            print(f"Row count: ok ({left_count} rows)")
        return same_rowcount

    @staticmethod
    def _build_diff_dataframe(left_df: DataFrame, right_df: DataFrame, join_cols: List[str]) -> DataFrame:
        """Performs a column-by-column comparison between two DataFrames.
        The two DataFrames must have the same columns with the same ordering.
        The column `join_col` will be used to join the two DataFrames together.
        Then we build a new DataFrame with the `join_col` and for each column, a struct with three elements:
        - `left_value`: the value coming from the `left_df`
        - `right_value`: the value coming from the `right_df`
        - `same_hash`: True if both values have the same hash, False otherwise.

        Example:

        >>> spark = spark_utils.get_spark_session("doctest")
        >>> left_df = spark.createDataFrame([
        ...         (1, 'a', 'x', 1),
        ...         (2, 'b', 'y', 2),
        ...         (3, 'c', 'z', 3)
        ...     ], "id INT, c1 STRING, c2 STRING, c3 INT"
        ... )
        >>> right_df = spark.createDataFrame([
        ...         (1, 'd', 'x', 1),
        ...         (2, 'a', 'y', 4),
        ...         (3, 'f', 'z', 3)
        ...     ], "id INT, c1 STRING, c2 STRING, c3 INT"
        ... )
        >>> left_df.show()
        +---+---+---+---+
        | id| c1| c2| c3|
        +---+---+---+---+
        |  1|  a|  x|  1|
        |  2|  b|  y|  2|
        |  3|  c|  z|  3|
        +---+---+---+---+
        <BLANKLINE>
        >>> right_df.show()
        +---+---+---+---+
        | id| c1| c2| c3|
        +---+---+---+---+
        |  1|  d|  x|  1|
        |  2|  a|  y|  4|
        |  3|  f|  z|  3|
        +---+---+---+---+
        <BLANKLINE>
        >>> from karadoc.common.nonreg import DataframeComparator
        >>> DataframeComparator._build_diff_dataframe(left_df, right_df, ['id']).orderBy('id').show()
        +---+-------------+------------+-------------+
        | id|           c1|          c2|           c3|
        +---+-------------+------------+-------------+
        |  1|{a, d, false}|{x, x, true}| {1, 1, true}|
        |  2|{b, a, false}|{y, y, true}|{2, 4, false}|
        |  3|{c, f, false}|{z, z, true}| {3, 3, true}|
        +---+-------------+------------+-------------+
        <BLANKLINE>

        :param left_df: a DataFrame
        :param right_df: a DataFrame with the same columns
        :param join_cols: the columns to use to perform the join.
        :return: a DataFrame containing all the columns that differ, and a dictionary that gives the number of
            differing rows for each column
        """

        def get_hash(field: StructField, df: DataFrame) -> Column:
            return f.hash(*DataframeComparator._canonize_col(field, df[quote(field.name)]))

        diff = left_df.join(right_df, join_cols, "inner")

        diff_df = diff.select(
            *quote_columns(join_cols),
            *[
                f.struct(
                    left_df[quote(field.name)].alias("left_value"),
                    right_df[quote(field.name)].alias("right_value"),
                    get_hash(field, left_df).eqNullSafe(get_hash(field, right_df)).alias("same_hash"),
                ).alias(field.name)
                for field in left_df.schema.fields
                if field.name not in join_cols
            ],
        )
        # There is no need to filter rows that are fully identical because it must have been done beforehand
        return diff_df.persist(StorageLevel.DISK_ONLY)

    def _unpivot(self, diff_df: DataFrame, join_cols: List[str]):
        """Given a diff_df, builds an unpivoted version of it.
        All the values must be cast to STRING to make sure everything fits in the same column.

        >>> diff_df = __get_test_diff_df()
        >>> diff_df.show()
        +---+-------------+------------+-------------+
        | id|           c1|          c2|           c3|
        +---+-------------+------------+-------------+
        |  1|{a, d, false}|{x, x, true}| {1, 1, true}|
        |  2|{b, a, false}|{y, y, true}|{2, 4, false}|
        |  3|{c, f, false}|{z, z, true}| {3, 3, true}|
        +---+-------------+------------+-------------+
        <BLANKLINE>

        >>> from karadoc.common.nonreg import DataframeComparator
        >>> DataframeComparator()._unpivot(diff_df, join_cols=['id']).orderBy('id', 'column').show()
        +---+------+-------------+
        | id|column|         diff|
        +---+------+-------------+
        |  1|    c1|{a, d, false}|
        |  1|    c2| {x, x, true}|
        |  1|    c3| {1, 1, true}|
        |  2|    c1|{b, a, false}|
        |  2|    c2| {y, y, true}|
        |  2|    c3|{2, 4, false}|
        |  3|    c1|{c, f, false}|
        |  3|    c2| {z, z, true}|
        |  3|    c3| {3, 3, true}|
        +---+------+-------------+
        <BLANKLINE>

        :param diff_df:
        :param join_cols:
        :return:
        """

        def truncate_string(col):
            return f.when(
                f.length(col) > f.lit(self.max_string_length),
                f.concat(f.substring(col, 0, self.max_string_length - 3), f.lit("...")),
            ).otherwise(col)

        diff_df = diff_df.select(
            *quote_columns(join_cols),
            *[
                f.struct(
                    truncate_string(diff_df[quote(column) + ".left_value"].cast(StringType())).alias("left_value"),
                    truncate_string(diff_df[quote(column) + ".right_value"].cast(StringType())).alias("right_value"),
                    diff_df[quote(column) + ".same_hash"].alias("same_hash"),
                ).alias(column)
                for column in diff_df.columns
                if column not in join_cols
            ],
        )
        unpivot = spark_utils.unpivot(diff_df, pivot_columns=join_cols, key_alias="column", value_alias="diff")
        # #102035
        # We persist the unpivoted dataframe as a workaround in spark 3.1.2 because we faced OOM errors
        # in the spark optimization of the query execution plan. We can remove this workaround on spark 3.1.3
        return unpivot.persist(StorageLevel.DISK_ONLY)

    @staticmethod
    def _build_diff_per_column_df(unpivoted_diff_df: DataFrame, nb_diffed_rows) -> DataFrame:
        """Given an `unpivoted_diff_df` DataFrame, builds a DataFrame that gives for each columns the N most frequent
        differences that are happening, where N = `nb_diffed_rows`.

        Example:

        >>> spark = spark_utils.get_spark_session("doctest")
        >>> unpivoted_diff_df = spark.createDataFrame([
        ...         (1, "c1", {'left_value': 'a', 'right_value': 'd', 'same_hash': False}),
        ...         (1, "c2", {'left_value': 'x', 'right_value': 'x', 'same_hash': True}),
        ...         (1, "c3", {'left_value': '1', 'right_value': '1', 'same_hash': True}),
        ...         (2, "c1", {'left_value': 'b', 'right_value': 'a', 'same_hash': False}),
        ...         (2, "c2", {'left_value': 'y', 'right_value': 'y', 'same_hash': True}),
        ...         (2, "c3", {'left_value': '2', 'right_value': '4', 'same_hash': False}),
        ...         (3, "c1", {'left_value': 'c', 'right_value': 'f', 'same_hash': False}),
        ...         (3, "c2", {'left_value': 'z', 'right_value': 'z', 'same_hash': True}),
        ...         (3, "c3", {'left_value': '3', 'right_value': '3', 'same_hash': True})
        ...     ], "id INT, column STRING, diff STRUCT<left_value: STRING, right_value: STRING, same_hash: BOOLEAN>"
        ... )
        >>> unpivoted_diff_df.show()
        +---+------+-------------+
        | id|column|         diff|
        +---+------+-------------+
        |  1|    c1|{a, d, false}|
        |  1|    c2| {x, x, true}|
        |  1|    c3| {1, 1, true}|
        |  2|    c1|{b, a, false}|
        |  2|    c2| {y, y, true}|
        |  2|    c3|{2, 4, false}|
        |  3|    c1|{c, f, false}|
        |  3|    c2| {z, z, true}|
        |  3|    c3| {3, 3, true}|
        +---+------+-------------+
        <BLANKLINE>
        >>> from karadoc.common.nonreg import DataframeComparator
        >>> DataframeComparator._build_diff_per_column_df(unpivoted_diff_df, 1).orderBy('column').show()
        +------+----------+-----------+--------------+--------------------+
        |column|left_value|right_value|nb_differences|total_nb_differences|
        +------+----------+-----------+--------------+--------------------+
        |    c1|         a|          d|             1|                   3|
        |    c3|         2|          4|             1|                   1|
        +------+----------+-----------+--------------+--------------------+
        <BLANKLINE>

        :param unpivoted_diff_df: a diff DataFrame
        :return: a dict that gives for each column with differences the number of rows with different values
          for this column
        """
        # We must make sure to break ties on nb_differences when ordering to ensure a deterministic for unit tests.
        window = Window.partitionBy(f.col("column")).orderBy(
            f.col("nb_differences").desc(), f.col("left_value"), f.col("right_value")
        )

        df = (
            unpivoted_diff_df.filter("diff.same_hash = false")
            .groupBy("column", "diff.left_value", "diff.right_value")
            .agg(f.count(f.lit(1)).alias("nb_differences"))
            .withColumn("row_num", f.row_number().over(window))
            .withColumn("total_nb_differences", f.sum("nb_differences").over(Window.partitionBy("column")))
            .where(f.col("row_num") <= f.lit(nb_diffed_rows))
            .drop("row_num")
        )

        return df

    @staticmethod
    def _row_frequency_count(df: DataFrame) -> DataFrame:
        """Given a DataFrame, returns another DataFrame with the number of occurrences of each rows,
        sorted by most frequent rows.

        >>> spark = spark_utils.get_spark_session("doctest")
        >>> df = spark.createDataFrame([
        ...         ("a", "b"),
        ...         ("a", "b"),
        ...         ("a", "b"),
        ...         ("a", "c"),
        ...         ("b", "d"),
        ...     ], "left_col: STRING, right_col: STRING"
        ... )
        >>> df.show()
        +--------+---------+
        |left_col|right_col|
        +--------+---------+
        |       a|        b|
        |       a|        b|
        |       a|        b|
        |       a|        c|
        |       b|        d|
        +--------+---------+
        <BLANKLINE>
        >>> from karadoc.common.nonreg import DataframeComparator
        >>> DataframeComparator._row_frequency_count(df).show()
        +--------+---------+---+
        |left_col|right_col| nb|
        +--------+---------+---+
        |       a|        b|  3|
        |       a|        c|  1|
        |       b|        d|  1|
        +--------+---------+---+
        <BLANKLINE>

        :param df:
        :return:
        """
        return df.groupBy(*quote_columns(df.columns)).agg(f.count(f.lit(1)).alias("nb")).orderBy(f.desc("nb"))

    def _format_diff_df(self, join_cols: List[str], diff_df: DataFrame) -> DataFrame:
        """Given a diff DataFrame, rename the columns to prefix them with the left_df_alias and right_df_alias.

        :param join_cols:
        :param diff_df:
        :return:
        """
        return diff_df.select(
            *quote_columns(join_cols),
            *[
                col
                for col_name in diff_df.columns
                if col_name not in join_cols
                for col in [
                    diff_df[quote(col_name)]["left_value"].alias(f"{self.left_df_alias}.{col_name}"),
                    diff_df[quote(col_name)]["right_value"].alias(f"{self.right_df_alias}.{col_name}"),
                ]
            ],
        )

    def _get_diff_count_per_col(self, diff_df: DataFrame, join_cols: List[str]) -> DataFrame:
        """Given a diff_df and its list of join_cols, return a DataFrame with the following properties:

        - One row per "diff tuple" (col_name, col_value_left, col_value_right)
        - A column nb_differences that gives the number of occurrence of each "diff tuple"
        - A column total_nb_differences that gives total number of differences found for this col_name.

        >>> _diff_df = __get_test_diff_df()
        >>> _diff_df.show()
        +---+-------------+------------+-------------+
        | id|           c1|          c2|           c3|
        +---+-------------+------------+-------------+
        |  1|{a, d, false}|{x, x, true}| {1, 1, true}|
        |  2|{b, a, false}|{y, y, true}|{2, 4, false}|
        |  3|{c, f, false}|{z, z, true}| {3, 3, true}|
        +---+-------------+------------+-------------+
        <BLANKLINE>
        >>> from karadoc.common.nonreg import DataframeComparator
        >>> df_comparator = DataframeComparator(left_df_alias="before", right_df_alias="after")
        >>> df_comparator._get_diff_count_per_col(_diff_df, join_cols = ['id']).show()
        +------+----------+-----------+--------------+--------------------+
        |column|left_value|right_value|nb_differences|total_nb_differences|
        +------+----------+-----------+--------------+--------------------+
        |    c1|         a|          d|             1|                   3|
        |    c1|         b|          a|             1|                   3|
        |    c1|         c|          f|             1|                   3|
        |    c3|         2|          4|             1|                   1|
        +------+----------+-----------+--------------+--------------------+
        <BLANKLINE>

        :param diff_df:
        :param join_cols:
        :return:
        """
        unpivoted_diff_df = self._unpivot(diff_df, join_cols)
        diff_count_per_col_df = (
            DataframeComparator._build_diff_per_column_df(unpivoted_diff_df, self.nb_diffed_rows)
            .orderBy("column")
            .cache()
        )
        return diff_count_per_col_df

    def _display_diff_results(self, diff_count_per_col_df: DataFrame, join_cols: List[str]):
        """Displays the results of the diff analysis.
        We first display the a summary of all columns that changed with the number of changes,
        then for each column, we display a summary of the most frequent changes and then
        we display examples of rows where this column changed, along with all the other columns
        that changed in this diff.

        Example:

        >>> spark = spark_utils.get_spark_session("doctest")
        >>> diff_count_per_col_df = spark.createDataFrame([
        ...         ('c1', 'a', 'd', 1, 3),
        ...         ('c3', '2', '4', 1, 1),
        ...     ], "column STRING, left_value STRING, right_value STRING, nb_differences INT, total_nb_differences INT"
        ... )
        >>> diff_count_per_col_df.show()
        +------+----------+-----------+--------------+--------------------+
        |column|left_value|right_value|nb_differences|total_nb_differences|
        +------+----------+-----------+--------------+--------------------+
        |    c1|         a|          d|             1|                   3|
        |    c3|         2|          4|             1|                   1|
        +------+----------+-----------+--------------+--------------------+
        <BLANKLINE>
        >>> from karadoc.common.nonreg import DataframeComparator
        >>> df_comparator = DataframeComparator(left_df_alias="before", right_df_alias="after")
        >>> df_comparator._display_diff_results(diff_count_per_col_df, join_cols = ['id'])
        Found the following differences by joining with column 'id'
        +-------------+---------------+------+-----+----------------+
        |`column name`|`total nb diff`|before|after|`nb differences`|
        +-------------+---------------+------+-----+----------------+
        |c1           |3              |a     |d    |1               |
        |c3           |1              |2     |4    |1               |
        +-------------+---------------+------+-----+----------------+
        <BLANKLINE>

        :param diff_count_per_col_df:
        :param join_cols:
        :return:
        """
        if len(join_cols) == 1:
            print(f"Found the following differences by joining with column '{join_cols[0]}'")
        else:
            print(f"Found the following differences by joining with columns: {join_cols}")
        diff_count_per_col_df.select(
            f.col("column").alias("`column name`"),
            f.col("total_nb_differences").alias("`total nb diff`"),
            f.col("left_value").alias(self.left_df_alias),
            f.col("right_value").alias(self.right_df_alias),
            f.col("nb_differences").alias("`nb differences`"),
        ).show(1000 * self.nb_diffed_rows, False)

    def _display_diff_examples(self, diff_df: DataFrame, diff_count_per_col_df: DataFrame, join_cols: List[str]):
        """For each column that has differences, print examples of rows where such a difference occurs.

        >>> _diff_df = __get_test_diff_df()
        >>> _diff_df.show()
        +---+-------------+------------+-------------+
        | id|           c1|          c2|           c3|
        +---+-------------+------------+-------------+
        |  1|{a, d, false}|{x, x, true}| {1, 1, true}|
        |  2|{b, a, false}|{y, y, true}|{2, 4, false}|
        |  3|{c, f, false}|{z, z, true}| {3, 3, true}|
        +---+-------------+------------+-------------+
        <BLANKLINE>
        >>> from karadoc.common.nonreg import DataframeComparator
        >>> df_comparator = DataframeComparator(left_df_alias="before", right_df_alias="after")
        >>> _diff_count_per_col_df = df_comparator._get_diff_count_per_col(_diff_df, join_cols = ['id'])
        >>> df_comparator._display_diff_examples(_diff_df, _diff_count_per_col_df, join_cols = ['id'])
        Detailed examples :
        'c1' : 3 rows
        +---+---------+--------+---------+--------+
        |id |before.c1|after.c1|before.c3|after.c3|
        +---+---------+--------+---------+--------+
        |1  |a        |d       |1        |1       |
        |2  |b        |a       |2        |4       |
        |3  |c        |f       |3        |3       |
        +---+---------+--------+---------+--------+
        <BLANKLINE>
        'c3' : 1 rows
        +---+---------+--------+---------+--------+
        |id |before.c1|after.c1|before.c3|after.c3|
        +---+---------+--------+---------+--------+
        |2  |b        |a       |2        |4       |
        +---+---------+--------+---------+--------+
        <BLANKLINE>

        :param diff_df:
        :param diff_count_per_col_df:
        :param join_cols:
        :return:
        """
        rows = diff_count_per_col_df.select("column", "total_nb_differences").distinct().collect()
        diff_count_per_col = [(r[0], r[1]) for r in rows]
        print("Detailed examples :")
        for col, nb in diff_count_per_col:
            print(f"'{col}' : {nb} rows")
            rows_that_changed_for_that_column = diff_df.filter(~diff_df[quote(col)]["same_hash"]).select(
                *join_cols, *[quote(r[0]) for r in rows]
            )
            self._format_diff_df(join_cols, rows_that_changed_for_that_column).show(self.nb_diffed_rows, False)

    def _analyze_differences(
        self, left_only: DataFrame, right_only: DataFrame, join_cols: List[str]
    ) -> Tuple[Optional[DataFrame], Optional[List[str]]]:
        """Performs an in-depth analysis between two DataFrames with the same columns and prints the differences found.
        We first attempt to identify columns that look like ids.
        For that we choose all the columns with an approximate_count_distinct greater than 90% of the row count.
        For each column selected this way, we then perform a join and compare the DataFrames column by column.

        :param left_only: a DataFrame
        :param right_only: another DataFrame with the same columns
        :param join_cols: the list of columns on which to perform the join
        :return: a Dict that gives for each eligible join column the corresponding diff DataFrame
        """
        if join_cols is None:
            inferred_join_col = DataframeComparator._automatically_infer_join_col(left_only, right_only)
            if inferred_join_col is not None:
                print(
                    f"We will try to find the differences by joining the DataFrames "
                    f"using this column: {inferred_join_col}"
                )
                return self._build_diff_dataframe(left_only, right_only, [inferred_join_col]), [inferred_join_col]
            else:
                print(
                    "Could not automatically infer a column for joining the DataFrames.\n"
                    "Please provide one by using the --nonreg-join-cols options or by setting the job.primary_key "
                    "in the table's POPULATE.py file."
                )
                return None, None
        else:
            print(
                f"We will try to find the differences by joining the DataFrames "
                f"using the provided columns together : {join_cols}"
            )
            return self._build_diff_dataframe(left_only, right_only, join_cols), join_cols

    def compare_df(
        self, left_df: DataFrame, right_df: DataFrame, join_cols: List[str] = None, show_examples: bool = False
    ) -> NonregResult:
        """Compares two DataFrames and print out the differences.
        We first compare the DataFrame schemas and row counts,
        then we compute the hashes of every rows in both DataFrames
        and compare the differences.
        If differences are found, a more in-depth analysis is performed
        to try to find which columns differ between the two DataFrames.

        If `join_cols` is specified, we will use the specified columns to perform the comparison join.
        If not, the columns to use for the join will be automatically inferred. If several columns are eligible for
        the join, the join will be performed on each of them. The automatic inference can only determine join based
        on a single column. If the DataFrame's unique keys are composite (multiple columns) they have to be given
        explicitely via `join_cols` to perform the diff analysis.

        Known Limitations :
        This method is able to compare Arrays and Maps while ignoring their ordering,
        It is able to handle nested non-repeated structures, such as STRUCT<STRUCT<>>, or even ARRAY<STRUCT>>
        but it doesn't support nested repeated structures, such as ARRAY<ARRAY<>>.

        :param left_df: a DataFrame
        :param right_df: another DataFrame
        :param join_cols: [Optional] specifies the columns on which the two DataFrames should be joined to compare them
        :param show_examples: if set to True, print for each column examples of full rows where this column changes
        :return: True if both DataFrames matches, False otherwise.
        """
        diff_df = None

        if join_cols == []:
            join_cols = None

        magic_hash_col_name = "MAGIC_HASH"

        same_schema = self._compare_schemas(left_df, right_df)

        if join_cols is not None:
            left_df = left_df.repartition(*join_cols)
        if join_cols is not None:
            right_df = right_df.repartition(*join_cols)

        left_flat = spark_utils.flatten(left_df)
        right_flat = spark_utils.flatten(right_df)

        if not same_schema:
            common_cols = spark_utils.get_common_columns(left_flat.schema, right_flat.schema)
            left_flat = left_flat.select(common_cols)
            right_flat = right_flat.select(common_cols)

        left_flat = left_flat.withColumn(
            magic_hash_col_name,
            f.hash(*[col for field in left_flat.schema.fields for col in self._canonize_col(field)]),
        )
        right_flat = right_flat.withColumn(
            magic_hash_col_name,
            f.hash(*[col for field in right_flat.schema.fields for col in self._canonize_col(field)]),
        )

        left_flat = left_flat.persist(StorageLevel.DISK_ONLY)
        right_flat = right_flat.persist(StorageLevel.DISK_ONLY)

        self._compare_rowcounts(left_flat, right_flat)

        left_hash = left_flat.select(magic_hash_col_name)
        right_hash = right_flat.select(magic_hash_col_name)

        left_hash_only = left_hash.join(right_hash, magic_hash_col_name, "leftanti")
        right_hash_only = right_hash.join(left_hash, magic_hash_col_name, "leftanti")

        left_hash_only_nb = left_hash_only.count()
        right_hash_only_nb = right_hash_only.count()

        if left_hash_only_nb > 0 or right_hash_only_nb > 0:
            print("Diff: not ok")
            if right_hash_only_nb > 0:
                print(f"{right_hash_only_nb} rows appeared")
            if left_hash_only_nb > 0:
                print(f"{left_hash_only_nb} rows disappeared")

            print("analyzing differences...")
            # We only compare the rows of the DataFrames with a hashcode that doesn't match the other DataFrame
            left_only_df = left_flat.join(left_hash_only, magic_hash_col_name, "leftsemi").drop(magic_hash_col_name)
            right_only_df = right_flat.join(right_hash_only, magic_hash_col_name, "leftsemi").drop(magic_hash_col_name)
            left_only_df = left_only_df.persist(StorageLevel.DISK_ONLY)
            right_only_df = right_only_df.persist(StorageLevel.DISK_ONLY)

            diff_df, join_cols = self._analyze_differences(left_only_df, right_only_df, join_cols)
            if diff_df is not None and diff_df.count() > 0:
                diff_count_per_col_df = self._get_diff_count_per_col(diff_df, join_cols)
                self._display_diff_results(diff_count_per_col_df, join_cols)
                if show_examples:
                    self._display_diff_examples(diff_df, diff_count_per_col_df, join_cols)

            left_only_df.unpersist()
            right_only_df.unpersist()
        else:
            print("Diff: ok")

        left_df.unpersist()
        right_df.unpersist()
        left_flat.unpersist()
        right_flat.unpersist()

        return NonregResult(same_schema, diff_df)


def __get_test_diff_df() -> DataFrame:
    spark = spark_utils.get_spark_session("doctest")
    diff_df = spark.createDataFrame(
        [
            (
                1,
                {"left_value": "a", "right_value": "d", "same_hash": False},
                {"left_value": "x", "right_value": "x", "same_hash": True},
                {"left_value": 1, "right_value": 1, "same_hash": True},
            ),
            (
                2,
                {"left_value": "b", "right_value": "a", "same_hash": False},
                {"left_value": "y", "right_value": "y", "same_hash": True},
                {"left_value": 2, "right_value": 4, "same_hash": False},
            ),
            (
                3,
                {"left_value": "c", "right_value": "f", "same_hash": False},
                {"left_value": "z", "right_value": "z", "same_hash": True},
                {"left_value": 3, "right_value": 3, "same_hash": True},
            ),
        ],
        "id INT,"
        + "c1 STRUCT<left_value: STRING, right_value: STRING, same_hash: BOOLEAN>,"
        + "c2 STRUCT<left_value: STRING, right_value: STRING, same_hash: BOOLEAN>,"
        + "c3 STRUCT<left_value: INT, right_value: INT, same_hash: BOOLEAN>",
    )
    return diff_df
