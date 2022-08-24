from typing import List

from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

from karadoc.common import spark_utils
from karadoc.common.spark_utils import quote
from karadoc.common.utils.assert_utils import assert_true


def _date_cols_to_numeric(df: DataFrame, reference_time_col: str, ignore_columns: List[str]) -> DataFrame:
    """Replace all date columns with the time difference in days between them and the reference time.

    :param df: a DataFrame
    :param reference_time_col: name of the column to use as reference time
    :param ignore_columns: List of column names to ignore
    :return:
    """
    df_res = df
    date_types = ["timestamp", "date"]
    date_cols = [
        col_name
        for col_name, type in df.dtypes
        if type in date_types and col_name not in [reference_time_col] + ignore_columns
    ]
    for col in date_cols:
        df_res = df_res.withColumn(col, f.datediff(f.col(reference_time_col), f.col(col)))

    return df_res


def _get_always_null_columns(df: DataFrame) -> List[str]:
    """Compute the list of all columns that are always null.

    Example:
    >>> spark = spark_utils.get_spark_session("doctest")
    >>> df = spark.createDataFrame([
    ...         (1, 'a', None, None),
    ...         (2, 'b', None, None),
    ...         (3, 'c', None, None)
    ...     ], "id INT, c1 STRING, c2 STRING, c3 STRING"
    ... )
    >>> always_null_cols = _get_always_null_columns(df)
    >>> always_null_cols.sort()
    >>> print(always_null_cols)
    ['c2', 'c3']

    :param df: a DataFrame
    :return: a list with the name of the columns that are always null
    """
    count_null_df = df.select([f.sum(f.col(col).isNotNull().astype(IntegerType())).alias(col) for col in df.columns])
    row = count_null_df.collect()[0]
    return [col_name for col_name, count in row.asDict().items() if count == 0]


def _quantile_discretize_numeric_col(df: DataFrame, nb_buckets: int, ignore_columns: List[str]) -> DataFrame:
    """Replace all numerical columns with their value discretized by quantile.
    Every column with a numeric type will be converted to bucketized string column.
    :param df: a DataFrame
    :param nb_buckets: the number of buckets to make
    :return: a DataFrame with the same column names, with numerical columns converted into string buckets.
    """
    num_types = ["bigint", "double", "int", "float"]  # excluding 'boolean'
    num_cols = [col_name for col_name, type in df.dtypes if type in num_types and col_name not in ignore_columns]

    for col, type in df.dtypes:
        if type == "boolean" or col in ignore_columns:
            df = df.withColumn(col, f.col(col).cast("string"))

    df_res = df
    for col in num_cols:
        window = Window.partitionBy(f.col("disc_" + col))
        max_digits = len(str(df_res.agg(f.max(f.abs(df_res[col]))).collect()[0][0]))
        df_res = (
            QuantileDiscretizer(numBuckets=nb_buckets, inputCol=col, outputCol="disc_" + col)
            .fit(df_res)
            .transform(df_res)
            .withColumn("max" + col, f.lpad(f.max(col).over(window), max_digits, "0"))
            .withColumn("min" + col, f.lpad(f.min(col).over(window), max_digits, "0"))
            .withColumn(col, f.concat(f.col("min" + col), f.lit("_to_"), f.col("max" + col)))
            .drop(
                "disc_" + col,
                "max" + col,
                "min" + col,
            )
        )
    return df_res


def _compute_frequencies(df: DataFrame, nb_top_values: int) -> DataFrame:
    """Compute the frequency of all key and values, grouped by cohort.
    Only the most frequent values will be kept, up to `nb_top_values`.
    Other values will be replaced by the null and the flag is_other will be set to true.

    Example:

    >>> spark = spark_utils.get_spark_session("doctest")
    >>> df = spark.createDataFrame([
    ...          ('2017-07-24 00:00:00', 'string_col', 'DOGS'),
    ...          ('2017-07-24 00:00:00', 'string_col', 'BIRDS'),
    ...          ('2017-07-24 00:00:00', 'string_col', None),
    ...          ('2017-07-24 00:00:00', 'always_null_col', None),
    ...          ('2017-07-24 00:00:00', 'always_null_col', None),
    ...          ('2017-07-31 00:00:00', 'string_col', 'CATS'),
    ...          ('2017-07-31 00:00:00', 'string_col', 'CATS'),
    ...          ('2017-07-31 00:00:00', 'string_col', 'CATS'),
    ...     ], "cohort STRING, key STRING, val STRING"
    ... )
    >>> df.orderBy('cohort', 'key', 'val').show()
    +-------------------+---------------+-----+
    |             cohort|            key|  val|
    +-------------------+---------------+-----+
    |2017-07-24 00:00:00|always_null_col| null|
    |2017-07-24 00:00:00|always_null_col| null|
    |2017-07-24 00:00:00|     string_col| null|
    |2017-07-24 00:00:00|     string_col|BIRDS|
    |2017-07-24 00:00:00|     string_col| DOGS|
    |2017-07-31 00:00:00|     string_col| CATS|
    |2017-07-31 00:00:00|     string_col| CATS|
    |2017-07-31 00:00:00|     string_col| CATS|
    +-------------------+---------------+-----+
    <BLANKLINE>
    >>> _compute_frequencies(df, 1).orderBy('cohort', 'key', 'val', 'is_other').show()
    +-------------------+---------------+----+--------+---------+------------------+
    |             cohort|            key| val|is_other|val_count|     val_frequency|
    +-------------------+---------------+----+--------+---------+------------------+
    |2017-07-24 00:00:00|always_null_col|null|   false|        2|               1.0|
    |2017-07-24 00:00:00|     string_col|null|   false|        1|0.3333333333333333|
    |2017-07-24 00:00:00|     string_col|null|    true|        2|0.6666666666666666|
    |2017-07-24 00:00:00|     string_col|CATS|   false|        0|               0.0|
    |2017-07-31 00:00:00|always_null_col|null|   false|        0|              null|
    |2017-07-31 00:00:00|     string_col|null|   false|        0|               0.0|
    |2017-07-31 00:00:00|     string_col|null|    true|        0|               0.0|
    |2017-07-31 00:00:00|     string_col|CATS|   false|        3|               1.0|
    +-------------------+---------------+----+--------+---------+------------------+
    <BLANKLINE>

    :param df: a DataFrame with the following schema: "cohort STRING, key STRING, val STRING"
    :param nb_top_values: The number of most frequent values to keep.
    :return: a DataFrame with the following schema:
        "cohort STRING, key STRING, val STRING, is_other BOOLEAN, val_count BIGINT, val_frequency DOUBLE"
    """
    assert_true(df.columns == ["cohort", "key", "val"], df.columns)
    # nb_top_values = 1
    # df.orderBy('cohort', 'key', 'val').show()
    # +-------------------+---------------+-----+
    # |             cohort|            key|  val|
    # +-------------------+---------------+-----+
    # |2017-07-24 00:00:00|always_null_col| null|
    # |2017-07-24 00:00:00|always_null_col| null|
    # |2017-07-24 00:00:00|     string_col| null|
    # |2017-07-24 00:00:00|     string_col|BIRDS|
    # |2017-07-24 00:00:00|     string_col| DOGS|
    # |2017-07-31 00:00:00|     string_col| CATS|
    # |2017-07-31 00:00:00|     string_col| CATS|
    # |2017-07-31 00:00:00|     string_col| CATS|
    # +-------------------+---------------+-----+

    cohort_key_val_count_df = df.groupBy("cohort", "key", "val").agg(f.count(f.lit(1)).alias("count"))
    assert_true(cohort_key_val_count_df.columns == ["cohort", "key", "val", "count"], cohort_key_val_count_df.columns)
    # cohort_key_val_count_df.orderBy('cohort', 'key', 'val').show()
    # +-------------------+---------------+-----+-----+
    # |             cohort|            key|  val|count|
    # +-------------------+---------------+-----+-----+
    # |2017-07-24 00:00:00|always_null_col| null|    2|
    # |2017-07-24 00:00:00|     string_col| null|    1|
    # |2017-07-24 00:00:00|     string_col|BIRDS|    1|
    # |2017-07-24 00:00:00|     string_col| DOGS|    1|
    # |2017-07-31 00:00:00|     string_col| CATS|    3|
    # +-------------------+---------------+-----+-----+

    most_frequent_values = _get_most_frequent_values(cohort_key_val_count_df, nb_top_values)
    assert_true(most_frequent_values.columns == ["key", "val"], most_frequent_values.columns)
    # most_frequent_values.orderBy('key', 'val').show()
    # +---------------+----+
    # |            key| val|
    # +---------------+----+
    # |always_null_col|null|
    # |     string_col|null|
    # |     string_col|CATS|
    # +---------------+----+

    # We flag all the values that are not part of the most frequent values as 'other'
    join_df = (
        cohort_key_val_count_df.alias("T1")
        .join(most_frequent_values.alias("T2"), f.expr("T1.key = T2.key AND T1.val = T2.val"), "left_outer")
        .selectExpr("T1.cohort", "T1.key", "T2.val", "T2.val IS NULL AND T1.val IS NOT NULL as is_other", "count")
    )

    assert_true(join_df.columns == ["cohort", "key", "val", "is_other", "count"], join_df.columns)
    # join_df.orderBy('cohort', 'key', 'val', 'is_other').show()
    # +-------------------+---------------+----+--------+-----+
    # |             cohort|            key| val|is_other|count|
    # +-------------------+---------------+----+--------+-----+
    # |2017-07-24 00:00:00|always_null_col|null|   false|    2|
    # |2017-07-24 00:00:00|     string_col|null|   false|    1|
    # |2017-07-24 00:00:00|     string_col|null|    true|    1|
    # |2017-07-24 00:00:00|     string_col|null|    true|    1|
    # |2017-07-31 00:00:00|     string_col|CATS|   false|    3|
    # +-------------------+---------------+----+--------+-----+

    # We sum together all the occurrences of others
    sum_df = join_df.groupby(["cohort", "key", "val", "is_other"]).agg(f.sum("count").alias("count"))

    assert_true(sum_df.columns == ["cohort", "key", "val", "is_other", "count"], sum_df.columns)
    # sum_df.orderBy('cohort', 'key', 'val', 'is_other').show()
    # +-------------------+---------------+----+--------+-----+
    # |             cohort|            key| val|is_other|count|
    # +-------------------+---------------+----+--------+-----+
    # |2017-07-24 00:00:00|always_null_col|null|   false|    2|
    # |2017-07-24 00:00:00|     string_col|null|   false|    1|
    # |2017-07-24 00:00:00|     string_col|null|    true|    2|
    # |2017-07-31 00:00:00|     string_col|CATS|   false|    3|
    # +-------------------+---------------+----+--------+-----+

    distinct_key_val_df = sum_df.select("key", "val", "is_other").distinct()

    assert_true(distinct_key_val_df.columns == ["key", "val", "is_other"], distinct_key_val_df.columns)
    # distinct_key_val_df.orderBy('key', 'val', 'is_other').show()
    # +---------------+----+--------+
    # |            key| val|is_other|
    # +---------------+----+--------+
    # |always_null_col|null|   false|
    # |     string_col|null|   false|
    # |     string_col|null|    true|
    # |     string_col|CATS|   false|
    # +---------------+----+--------+

    count_per_cohort_df = (
        cohort_key_val_count_df.groupBy("cohort")
        .agg(f.sum(f.col("count")).alias("cohort_count"))
        .select("cohort", "cohort_count")
    )
    assert_true(count_per_cohort_df.columns == ["cohort", "cohort_count"], count_per_cohort_df.columns)
    # count_per_cohort_df.orderBy('cohort').show()
    # +-------------------+------------+
    # |             cohort|cohort_count|
    # +-------------------+------------+
    # |2017-07-24 00:00:00|           5|
    # |2017-07-31 00:00:00|           3|
    # +-------------------+------------+

    # Make a cross-join to build all possible couples 'cohort', 'key', 'val', 'is_other' to fill the holes
    canvas_df = distinct_key_val_df.crossJoin(count_per_cohort_df).select("cohort", "key", "val", "is_other")

    assert_true(canvas_df.columns == ["cohort", "key", "val", "is_other"], canvas_df.columns)
    # canvas_df.orderBy('cohort', 'key', 'val', 'is_other').show()
    # +-------------------+---------------+----+--------+
    # |             cohort|            key| val|is_other|
    # +-------------------+---------------+----+--------+
    # |2017-07-24 00:00:00|always_null_col|null|   false|
    # |2017-07-24 00:00:00|     string_col|null|   false|
    # |2017-07-24 00:00:00|     string_col|null|    true|
    # |2017-07-24 00:00:00|     string_col|CATS|   false|
    # |2017-07-31 00:00:00|always_null_col|null|   false|
    # |2017-07-31 00:00:00|     string_col|null|   false|
    # |2017-07-31 00:00:00|     string_col|null|    true|
    # |2017-07-31 00:00:00|     string_col|CATS|   false|
    # +-------------------+---------------+----+--------+

    # We perform a full join of the DataFrame that counts the occurrence of each values
    # with the canvas DataFrame to fill the holes.
    # We also flag the non-null values that are not in the canvas as not being part of the most frequent values
    complete_df = (
        sum_df.alias("T1")
        .join(
            canvas_df.alias("T2"),
            f.expr(
                "T1.cohort <=> T2.cohort AND T1.key <=> T2.key AND T1.val <=> T2.val AND T1.is_other <=> T2.is_other"
            ),
            "right_outer",
        )
        .selectExpr(
            "COALESCE(T1.cohort, T2.cohort) as cohort",
            "COALESCE(T1.key, T2.key) as key",
            "COALESCE(T1.val, T2.val) as val",
            "COALESCE(T1.is_other, T2.is_other) as is_other",
            "COALESCE(T1.count, 0) as val_count",
        )
    )

    assert_true(complete_df.columns == ["cohort", "key", "val", "is_other", "val_count"], complete_df.columns)
    # complete_df.orderBy('cohort', 'key', 'val', 'is_other').show()
    # +-------------------+---------------+----+--------+---------+
    # |             cohort|            key| val|is_other|val_count|
    # +-------------------+---------------+----+--------+---------+
    # |2017-07-24 00:00:00|always_null_col|null|   false|        2|
    # |2017-07-24 00:00:00|     string_col|null|   false|        1|
    # |2017-07-24 00:00:00|     string_col|null|    true|        2|
    # |2017-07-24 00:00:00|     string_col|CATS|   false|        0|
    # |2017-07-31 00:00:00|always_null_col|null|   false|        0|
    # |2017-07-31 00:00:00|     string_col|null|   false|        0|
    # |2017-07-31 00:00:00|     string_col|null|    true|        0|
    # |2017-07-31 00:00:00|     string_col|CATS|   false|        3|
    # +-------------------+---------------+----+--------+---------+

    frequency_df = complete_df.select(
        "cohort",
        "key",
        "val",
        "is_other",
        "val_count",
        (f.col("val_count") / f.sum("val_count").over(Window.partitionBy("cohort", "key"))).alias("val_frequency"),
    )
    assert_true(
        frequency_df.columns
        == [
            "cohort",
            "key",
            "val",
            "is_other",
            "val_count",
            "val_frequency",
        ],
        frequency_df.columns,
    )
    # frequency_df.orderBy('cohort', 'key', 'val', 'is_other').show()
    # +-------------------+---------------+----+--------+---------+------------------+
    # |             cohort|            key| val|is_other|val_count|     val_frequency|
    # +-------------------+---------------+----+--------+---------+------------------+
    # |2017-07-24 00:00:00|always_null_col|null|   false|        2|               1.0|
    # |2017-07-24 00:00:00|     string_col|null|   false|        1|0.3333333333333333|
    # |2017-07-24 00:00:00|     string_col|null|    true|        2|0.6666666666666666|
    # |2017-07-24 00:00:00|     string_col|CATS|   false|        0|               0.0|
    # |2017-07-31 00:00:00|always_null_col|null|   false|        0|              null|
    # |2017-07-31 00:00:00|     string_col|null|   false|        0|               0.0|
    # |2017-07-31 00:00:00|     string_col|null|    true|        0|               0.0|
    # |2017-07-31 00:00:00|     string_col|CATS|   false|        3|               1.0|
    # +-------------------+---------------+----+--------+---------+------------------+
    # The null here is okay, as this is not supposed to happen in practice with a well-formed DataFrame
    # It happens because there is now value for key 'always_null_col' in cohort '2017-07-31 00:00:00',
    # but in practice, if the cohort exists then there is at least one value for this key.

    return frequency_df


def _get_most_frequent_values(df: DataFrame, nb_top_values: int) -> DataFrame:
    """Compute the most frequent values for each column

    Example:

    >>> spark = spark_utils.get_spark_session("doctest")
    >>> df = spark.createDataFrame([
    ...          ('2017-07-24 00:00:00', 'always_null_col', None, 2),
    ...          ('2017-07-24 00:00:00', 'string_col', 'DOGS', 2),
    ...          ('2017-07-24 00:00:00', 'string_col', None, 1),
    ...          ('2017-07-31 00:00:00', 'string_col', 'CATS', 3),
    ...     ], "cohort STRING, key STRING, val STRING, count INT"
    ... )
    >>> df.show()
    +-------------------+---------------+----+-----+
    |             cohort|            key| val|count|
    +-------------------+---------------+----+-----+
    |2017-07-24 00:00:00|always_null_col|null|    2|
    |2017-07-24 00:00:00|     string_col|DOGS|    2|
    |2017-07-24 00:00:00|     string_col|null|    1|
    |2017-07-31 00:00:00|     string_col|CATS|    3|
    +-------------------+---------------+----+-----+
    <BLANKLINE>
    >>> _get_most_frequent_values(df, 1).orderBy('key', 'val').show()
    +---------------+----+
    |            key| val|
    +---------------+----+
    |always_null_col|null|
    |     string_col|null|
    |     string_col|CATS|
    +---------------+----+
    <BLANKLINE>

    :param df: a DataFrame with the following schema: "cohort STRING, key STRING, val STRING, count INT"
    :param nb_top_values: the max number of values to keep. Nulls values are kept.
    :return: a new DataFrame
    """
    # Example:
    # nb_top_values = 1
    assert_true(df.columns == ["cohort", "key", "val", "count"])
    key_val_count_df = df.groupBy("key", "val").agg(f.sum("count").alias("count"))
    # key_val_count_df.show()
    # +---------------+----+-----+
    # |            key| val|count|
    # +---------------+----+-----+
    # |     string_col|null|    1|
    # |     string_col|DOGS|    2|
    # |     string_col|CATS|    3|
    # |always_null_col|null|    2|
    # +---------------+----+-----+

    # null values should not count in the top most frequent value ranking
    non_null_val_count_col = f.when(f.col("val").isNull(), f.lit(None)).otherwise(f.col("count"))

    window = Window.partitionBy("key").orderBy(non_null_val_count_col.desc())
    most_frequent_values_df = key_val_count_df.withColumn("row_number", f.row_number().over(window)).where(
        (f.col("row_number") <= f.lit(nb_top_values)) | (f.col("val").isNull())
    )

    # most_frequent_values_df.show()
    # +---------------+----+-----+----------+
    # |            key| val|count|row_number|
    # +---------------+----+-----+----------+
    # |     string_col|CATS|    3|         1|
    # |     string_col|null|    1|         3|
    # |always_null_col|null|    2|         1|
    # +---------------+----+-----+----------+

    return most_frequent_values_df.drop("count").drop("row_number")


def _prefix_columns(df: DataFrame) -> DataFrame:
    """Add an underscore prefix to all columns of the given DataFrame
    This is useful to avoid naming collisions

    Example:
    >>> spark = spark_utils.get_spark_session("doctest")
    >>> df = spark.createDataFrame([], "a STRING, b STRING, c STRING, d INT")
    >>> _prefix_columns(df).columns
    ['_a', '_b', '_c', '_d']

    :param df: a DataFrame
    :return: another DataFrame with column renamed
    """
    res = df
    for col in df.columns:
        res = res.withColumnRenamed(col, "_" + col)
    return res


def _prepare_dataframe(df: DataFrame, cohort_cols: List[str]) -> DataFrame:
    """Transform the given Dataframe to make it usable in the analyzis algorithm.
    All the column names are first prefixed with an underscore.
    Then all the cohort columns are wrapped together in a struct column called 'cohort'.

    :param df: a DataFrame
    :param cohort_cols: names of the columns that will be used as cohorts
    :return:
    """
    df2 = _prefix_columns(df)
    cohort_cols = ["_" + col for col in cohort_cols]
    df3 = df2.select(
        f.struct(*[f.col(quote(col)).alias(col[1:]) for col in cohort_cols]).alias("cohort"),
        *[col for col in df2.columns if col not in cohort_cols],
    )
    return df3


def _finalize_dataframe(df: DataFrame) -> DataFrame:
    """Unwraps the cohort column.

    :param df: a DataFrame
    :return:
    """
    return df.select("cohort.*", *df.columns[1:])


def analyse_timeline(df: DataFrame, reference_time_col: str, cohort_cols: List[str], nb_buckets: int = 5) -> DataFrame:
    """Perform an analysis on the given DataFrame that computes the distribution with respect
        to the columns in the 'cohort_cols' list of each other column of the DataFrame.

        Each non-cohort column will be discretized with the following algorithm:
        - Date and Datetime columns are converted in numbers as the difference in days with the `reference_time_col`.
        - Numerical columns are discretized using a Quantile Discretizer algorithm that generates `nb_buckets` buckets.
          They are then cast into strings representing the bucket ranges they fall into.
        - For discretized columns like string, only the top `nb_buckets` frequent values (and the null values) are
          kept as buckets. All remaining values are aggregated a null with the column is_other set to true.

        We then return a DataFrame that gives for each column name, each cohort, each value and each is_other (boolean)
        their number of occurrences and relative frequency relatively to other values in that cohort.
        When a value doesn't appear in a cohort group, a row is still added with a 0 count, to allow for easy plotting.

    Example:
    >>> spark = spark_utils.get_spark_session("doctest")
    >>> df = spark.createDataFrame([
    ...          ('2017-07-24 00:00:00', 'DOGS',  1, None),
    ...          ('2017-07-24 00:00:00', 'DOGS',  1, None),
    ...          ('2017-07-24 00:00:00', 'CATS',  2, None),
    ...          ('2017-07-24 00:00:00', 'BIRDS', 2, None),
    ...          ('2017-07-24 00:00:00', None,    3, None),
    ...          ('2017-07-31 00:00:00', 'CATS',  4, None),
    ...          ('2017-07-31 00:00:00', 'CATS',  5, None),
    ...          ('2017-07-31 00:00:00', 'CATS',  6, None),
    ...     ], "cohort STRING, col_string STRING, col_int INT, col_null STRING"
    ... )
    >>> df.show()
    +-------------------+----------+-------+--------+
    |             cohort|col_string|col_int|col_null|
    +-------------------+----------+-------+--------+
    |2017-07-24 00:00:00|      DOGS|      1|    null|
    |2017-07-24 00:00:00|      DOGS|      1|    null|
    |2017-07-24 00:00:00|      CATS|      2|    null|
    |2017-07-24 00:00:00|     BIRDS|      2|    null|
    |2017-07-24 00:00:00|      null|      3|    null|
    |2017-07-31 00:00:00|      CATS|      4|    null|
    |2017-07-31 00:00:00|      CATS|      5|    null|
    |2017-07-31 00:00:00|      CATS|      6|    null|
    +-------------------+----------+-------+--------+
    <BLANKLINE>
    >>> analyse_timeline(df, 'cohort', ['cohort'], 2).orderBy('cohort', 'key', 'val', 'is_other').show()
    +-------------------+----------+------+--------+---------+-------------+
    |             cohort|       key|   val|is_other|val_count|val_frequency|
    +-------------------+----------+------+--------+---------+-------------+
    |2017-07-24 00:00:00|   col_int|1_to_1|   false|        2|          0.4|
    |2017-07-24 00:00:00|   col_int|2_to_6|   false|        3|          0.6|
    |2017-07-24 00:00:00|  col_null|  null|   false|        5|          1.0|
    |2017-07-24 00:00:00|col_string|  null|   false|        1|          0.2|
    |2017-07-24 00:00:00|col_string|  null|    true|        1|          0.2|
    |2017-07-24 00:00:00|col_string|  CATS|   false|        1|          0.2|
    |2017-07-24 00:00:00|col_string|  DOGS|   false|        2|          0.4|
    |2017-07-31 00:00:00|   col_int|1_to_1|   false|        0|          0.0|
    |2017-07-31 00:00:00|   col_int|2_to_6|   false|        3|          1.0|
    |2017-07-31 00:00:00|  col_null|  null|   false|        3|          1.0|
    |2017-07-31 00:00:00|col_string|  null|   false|        0|          0.0|
    |2017-07-31 00:00:00|col_string|  null|    true|        0|          0.0|
    |2017-07-31 00:00:00|col_string|  CATS|   false|        3|          1.0|
    |2017-07-31 00:00:00|col_string|  DOGS|   false|        0|          0.0|
    +-------------------+----------+------+--------+---------+-------------+
    <BLANKLINE>

    :param df: a DataFrame
    :param reference_time_col: The name of the column to use to normalize other date and datetime columns
    :param cohort_cols: The name of the columns to use as groups for the analysis
    :param nb_buckets: The number of distinct values to keep per column (nulls and 'OTHER' will also appear as values)
    :return: a DataFrame with the following columns: cohort_cols + ['key', 'val', 'val_count', 'val_frequency']
    """

    assert_true(
        reference_time_col in df.columns,
        "%s was not found in the dataframe's column: %s"
        % (
            reference_time_col,
            df.columns,
        ),
    )
    for col in cohort_cols:
        assert_true(col in df.columns, "%s was not found in the dataframe's column: %s" % (col, df.columns))

    df_res1 = _prepare_dataframe(df, cohort_cols)
    reference_time_col = "_" + reference_time_col

    df_res2 = _date_cols_to_numeric(df_res1, reference_time_col, cohort_cols)
    always_null_columns = _get_always_null_columns(df_res2)
    df_res3 = _quantile_discretize_numeric_col(df_res2, nb_buckets, always_null_columns).drop(reference_time_col)

    # Once the unpivot is done, we remove the underscore that we appended to the column names
    df_res4 = spark_utils.unpivot(df_res3, ["cohort"], key_alias="key", value_alias="val").withColumn(
        "key", f.expr("SUBSTR(key, 2)")
    )
    df_res5 = _compute_frequencies(df_res4, nb_buckets)

    df_res6 = _finalize_dataframe(df_res5)

    assert_true(df_res6.columns == cohort_cols + ["key", "val", "is_other", "val_count", "val_frequency"])
    return df_res6
