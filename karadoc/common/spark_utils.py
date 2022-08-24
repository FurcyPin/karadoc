import json
import re
from collections import OrderedDict
from copy import copy
from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple, Union, cast

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    ArrayType,
    DataType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from karadoc.common.utils.assert_utils import assert_true
from karadoc.spark.utils import get_spark_session  # noqa: F401 (used only in doctest)

OrderedTree = Union["OrderedTree", Dict[str, "OrderedTree"]]


def quote(col: str) -> str:
    """Puts the given column name into quotes.

    This is useful in particular when some column names contains dots,
    which usually happens after using the :func:`flatten` operation.

    Examples:

    >>> quote('a')
    '`a`'
    >>> quote('a.b')
    '`a.b`'

    Columns names can even contain backquotes, by escaping backquotes with another backquote.

    >>> quote('a`b')
    '`a``b`'

    Differs from `quote_idempotent` on this specific case
    >>> quote('`a`')
    '```a```'

    :param col:
    :return:
    """
    return "`" + col.replace("`", "``") + "`"


def quote_idempotent(col: str) -> str:
    """Puts the given column name into quotes if it is not already

    This is useful in particular when some column names contains dots,
    which usually happens after using the :func:`flatten` operation.

    Examples:

    >>> quote_idempotent('a')
    '`a`'
    >>> quote_idempotent('a.b')
    '`a.b`'

    Columns names can even contain backquotes, by escaping backquotes with another backquote.

    >>> quote_idempotent('a`b')
    '`a``b`'

    Differs from `quote` on this specific case
    >>> quote_idempotent('`a`')
    '`a`'

    :param col:
    :return:
    """
    return quote(unquote(col))


def quote_columns(columns: List[str]) -> List[str]:
    """Puts every column name of the given list into quotes.

    This is useful in particular when some column names contains dots,
    which usually happens after using the :func:`flatten` operation.

    :param columns:
    :return:
    """
    return [quote(col) for col in columns]


def unquote(col_name: str) -> str:
    """Removes quotes from a quoted column name.

    Examples:

    >>> unquote('a')
    'a'
    >>> unquote('`a`')
    'a'
    >>> unquote('`a.b`')
    'a.b'

    Columns names can even contain backquotes, by escaping backquotes with another backquote.

    >>> unquote('`a``b`')
    'a`b'

    :param col_name:
    :return:
    """
    if col_name[0] == "`" and col_name[-1] == "`":
        col_name = col_name[1:-1]
    return col_name.replace("``", "`")


def df_schema_as_json_string(df: DataFrame) -> str:
    schema_dict = json.loads(df.schema.json())
    return json.dumps(schema_dict, indent=2, sort_keys=True)


def get_schema_from_json(s: str) -> StructType:
    return StructType.fromJson(json.loads(s))


def extract_json_column(spark: SparkSession, df: DataFrame, columns: Union[str, List[str]]) -> DataFrame:
    """
    Transform the specified columns containing json strings in the given dataframe
    into structs containing the equivalent parsed information.

    This method uses Spark's json reader methods, but is more useful because Spark
    can only read json from an RDD[String]. This method has the advantage of allowing
    to extract several json columns and to keep the other Dataframe columns in the result.

    WARNING : when you use this method on a column that is inside a struct (e.g. column "a.b.c"),
    instead of replacing that column it will create a new column outside the struct (e.g. "`a.b.c`") (See Example 2).

    Example 1 :
    >>> spark = get_spark_session("doctest")
    >>> df = spark.createDataFrame([
    ...         (1, '[{"a": 1}, {"a": 2}]'),
    ...         (1, '[{"a": 2}, {"a": 4}]'),
    ...         (2, None)
    ...     ], "id INT, json1 STRING"
    ... )
    >>> df.show()
    +---+--------------------+
    | id|               json1|
    +---+--------------------+
    |  1|[{"a": 1}, {"a": 2}]|
    |  1|[{"a": 2}, {"a": 4}]|
    |  2|                null|
    +---+--------------------+
    <BLANKLINE>
    >>> df.printSchema()
    root
     |-- id: integer (nullable = true)
     |-- json1: string (nullable = true)
    <BLANKLINE>
    >>> extract_json_column(spark, df, 'json1').printSchema()
    root
     |-- id: integer (nullable = true)
     |-- json1: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- a: long (nullable = true)
    <BLANKLINE>

    Example 2 : json inside a struct :
    >>> df = spark.createDataFrame([
    ...         (1, {'json1': '[{"a": 1}, {"a": 2}]'}),
    ...         (1, {'json1': '[{"a": 2}, {"a": 4}]'}),
    ...         (2, None)
    ...     ], "id INT, struct STRUCT<json1: STRING>"
    ... )
    >>> df.show(10, False)
    +---+----------------------+
    |id |struct                |
    +---+----------------------+
    |1  |{[{"a": 1}, {"a": 2}]}|
    |1  |{[{"a": 2}, {"a": 4}]}|
    |2  |null                  |
    +---+----------------------+
    <BLANKLINE>
    >>> df.printSchema()
    root
     |-- id: integer (nullable = true)
     |-- struct: struct (nullable = true)
     |    |-- json1: string (nullable = true)
    <BLANKLINE>
    >>> res = extract_json_column(spark, df, 'struct.json1')
    >>> res.printSchema()
    root
     |-- id: integer (nullable = true)
     |-- struct: struct (nullable = true)
     |    |-- json1: string (nullable = true)
     |-- struct.json1: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- a: long (nullable = true)
    <BLANKLINE>
    >>> res.show(10, False)
    +---+----------------------+------------+
    |id |struct                |struct.json1|
    +---+----------------------+------------+
    |1  |{[{"a": 1}, {"a": 2}]}|[{1}, {2}]  |
    |1  |{[{"a": 2}, {"a": 4}]}|[{2}, {4}]  |
    |2  |null                  |null        |
    +---+----------------------+------------+
    <BLANKLINE>

    :param spark: the SparkContext
    :param df: a DataFrame
    :param columns: A list of column names
    :return: a new DataFrame
    """
    if type(columns) == str:
        columns = [columns]

    def wrap_json(col: str) -> Column:
        """Transforms a string column into the json string '{"col_name": col_value}'

        It is necessary to wrap the json because the schema inference is incorrect for json that are arrays

        Examples:
        df.show()
        +---+--------------------+
        | id|               json1|
        +---+--------------------+
        |  1|[{"a": 1}, {"a": 2}]|
        |  1|[{"a": 2}, {"a": 4}]|
        +---+--------------------+
        schema = job.spark.read.json(df.select('json1').rdd.map(lambda r: r[0])).schema
        schema.simpleString()
        returns 'struct<a:bigint>' but we the schema we need is 'array<struct<a:bigint>>'

        :param col:
        :return:
        """
        return f.concat(f.lit('{"'), f.lit(col), f.lit('": '), f.coalesce(col, f.lit("null")), f.lit("}"))

    res = df
    for col in columns:
        # >>> res.show()
        # +---+--------------------+
        # | id|               json1|
        # +---+--------------------+
        # |  1|[{"a": 1}, {"a": 2}]|
        # |  1|[{"a": 2}, {"a": 4}]|
        # |  2|                null|
        # +---+--------------------+
        res = res.withColumn(col, wrap_json(col))
        # >>> res.show(10, False)
        # +---+-------------------------------+
        # |id |json1                          |
        # +---+-------------------------------+
        # |1  |{"json1": [{"a": 1}, {"a": 2}]}|
        # |1  |{"json1": [{"a": 2}, {"a": 4}]}|
        # |2  |{"json1": null}                |
        # +---+-------------------------------+
        schema = spark.read.json(res.select(quote(col)).rdd.map(lambda r: r[0])).schema
        # >>> schema.simpleString()
        # 'struct<json1:array<struct<a:bigint>>>'
        res = res.withColumn(col, f.from_json(quote(col), schema).alias(col)[col])
        # >>> res.printSchema()
        # root
        #  |-- id: integer (nullable = true)
        #  |-- json1: array (nullable = true)
        #  |    |-- element: struct (containsNull = true)
        #  |    |    |-- a: long (nullable = true)
        # >>> res.show()
        # +---+----------+
        # | id|     json1|
        # +---+----------+
        # |  1|[[1], [2]]|
        # |  1|[[2], [4]]|
        # |  2|      null|
        # +---+----------+
    return res


def flatten_struct_in_df(struct_df: DataFrame, max_depth: int) -> DataFrame:
    """Flatten a struct column in a spark dataframe.

    >>> spark = get_spark_session("doctest")
    >>> df = spark.createDataFrame(
    ...     [
    ...         (1, {"a": 1, "b": {"c": 2, "d": 3}}),
    ...         (2, None),
    ...         (3, {"a": 1, "b": {"c": 2, "d": 3}})
    ...     ],
    ...     "id INT, s STRUCT<a:INT, b:STRUCT<c:INT, d:INT>>"
    ... )
    >>> df.printSchema()
    root
     |-- id: integer (nullable = true)
     |-- s: struct (nullable = true)
     |    |-- a: integer (nullable = true)
     |    |-- b: struct (nullable = true)
     |    |    |-- c: integer (nullable = true)
     |    |    |-- d: integer (nullable = true)
    <BLANKLINE>
    >>> df.show()
    +---+-----------+
    | id|          s|
    +---+-----------+
    |  1|{1, {2, 3}}|
    |  2|       null|
    |  3|{1, {2, 3}}|
    +---+-----------+
    <BLANKLINE>
    >>> flat_df = flatten_struct_in_df(df, 1)
    >>> flat_df.printSchema()
    root
     |-- id: integer (nullable = true)
     |-- s_a: integer (nullable = true)
     |-- s_b: struct (nullable = true)
     |    |-- c: integer (nullable = true)
     |    |-- d: integer (nullable = true)
    <BLANKLINE>
    >>> flat_df.show()
    +---+----+------+
    | id| s_a|   s_b|
    +---+----+------+
    |  1|   1|{2, 3}|
    |  2|null|  null|
    |  3|   1|{2, 3}|
    +---+----+------+
    <BLANKLINE>

    :param struct_df: a spark dataframe
    :param max_depth: The maximum depth to flatten recursively (ex: 2 will flatten 1 and 2)
    :return: A spark dataFrame
    """
    flat_cols, struct_cols, flat_df = [], [], []
    flat_cols.append([f.col(c.name) for c in struct_df.schema if not isinstance(c.dataType, StructType)])
    struct_cols.append([c.name for c in struct_df.schema if isinstance(c.dataType, StructType)])
    flat_df.append(
        struct_df.select(
            flat_cols[0]
            + [
                f.col(sc + "." + c).alias(sc + "_" + c)
                for sc in struct_cols[0]
                for c in struct_df.select(sc + ".*").columns
            ]
        )
    )
    for i in range(1, max_depth):
        flat_cols.append([f.col(c.name) for c in flat_df[i - 1].schema if not isinstance(c.dataType, StructType)])
        struct_cols.append([c.name for c in flat_df[i - 1].schema if isinstance(c.dataType, StructType)])
        flat_df.append(
            flat_df[i - 1].select(
                flat_cols[i]
                + [
                    f.col(sc + "." + c).alias(sc + "_" + c)
                    for sc in struct_cols[i]
                    for c in flat_df[i - 1].select(sc + ".*").columns
                ]
            )
        )
    return flat_df[-1]


def flatten(df: DataFrame, separator: str = ".") -> DataFrame:
    """Flattens all the struct columns of a DataFrame

    Nested fields names will be joined together using the specified separator

    Examples:

    >>> spark = get_spark_session("doctest")
    >>> df = spark.createDataFrame(
    ...         [(1, {"a": 1, "b": {"c": 1, "d": 1}})],
    ...         "id INT, s STRUCT<a:INT, b:STRUCT<c:INT, d:INT>>"
    ...      )
    >>> df.printSchema()
    root
     |-- id: integer (nullable = true)
     |-- s: struct (nullable = true)
     |    |-- a: integer (nullable = true)
     |    |-- b: struct (nullable = true)
     |    |    |-- c: integer (nullable = true)
     |    |    |-- d: integer (nullable = true)
    <BLANKLINE>
    >>> flatten(df).printSchema()
    root
     |-- id: integer (nullable = true)
     |-- s.a: integer (nullable = true)
     |-- s.b.c: integer (nullable = true)
     |-- s.b.d: integer (nullable = true)
    <BLANKLINE>
    >>> df = spark.createDataFrame(
    ...         [(1, {"a.a1": 1, "b.b1": {"c.c1": 1, "d.d1": 1}})],
    ...         "id INT, `s.s1` STRUCT<`a.a1`:INT, `b.b1`:STRUCT<`c.c1`:INT, `d.d1`:INT>>"
    ... )
    >>> df.printSchema()
    root
     |-- id: integer (nullable = true)
     |-- s.s1: struct (nullable = true)
     |    |-- a.a1: integer (nullable = true)
     |    |-- b.b1: struct (nullable = true)
     |    |    |-- c.c1: integer (nullable = true)
     |    |    |-- d.d1: integer (nullable = true)
    <BLANKLINE>
    >>> flatten(df, "?").printSchema()
    root
     |-- id: integer (nullable = true)
     |-- s.s1?a.a1: integer (nullable = true)
     |-- s.s1?b.b1?c.c1: integer (nullable = true)
     |-- s.s1?b.b1?d.d1: integer (nullable = true)
    <BLANKLINE>

    :param df: a DataFrame
    :param separator: It might be useful to change the separator when some DataFrame's column names already contain dots
    :return: a flattened DataFrame
    """
    # The idea is to recursively write a "SELECT s.b.c as `s.b.c`" for each nested column.
    cols = []

    def expand_struct(struct: StructType, col_stack: List[str]) -> None:
        for field in struct:
            if type(field.dataType) == StructType:
                expand_struct(field.dataType, col_stack + [field.name])
            else:
                column = f.col(".".join(quote_columns(col_stack + [field.name])))
                cols.append(column.alias(separator.join(col_stack + [field.name])))

    expand_struct(df.schema, col_stack=[])
    return df.select(cols)


def _build_nested_struct_tree(columns: List[str], struct_separator: str) -> OrderedTree:
    """Given a list of flattened column names and a separator

    >>> _build_nested_struct_tree(["id", "s.a", "s.b.c", "s.b.d"], ".")
    OrderedDict([('id', None), ('s', OrderedDict([('a', None), ('b', OrderedDict([('c', None), ('d', None)]))]))])

    :param columns: Name of the flattened columns
    :param struct_separator: Separator used in the column names for structs
    :return:
    """

    def rec_insert(node: OrderedTree, col: str) -> None:
        if struct_separator in col:
            struct, subcol = col.split(struct_separator, 1)
            if struct not in node:
                node[struct] = OrderedDict()
            rec_insert(node[struct], subcol)
        else:
            node[col] = None

    tree: OrderedTree = OrderedDict()
    for c in columns:
        rec_insert(tree, c)
    return tree


def _build_struct_from_tree(node: OrderedTree, separator: str, prefix: str = "") -> List[Column]:
    """Given an intermediate tree representing a nested struct, build a Spark Column
    that represents this nested structure.

    >>> spark = get_spark_session("doctest")
    >>> tree = OrderedDict([('b!', OrderedDict([
    ...      ('c', None),
    ...      ('d', None)
    ...    ]))])
    >>> _build_struct_from_tree(tree, ".") # noqa: E501
    [Column<'CASE WHEN ((true AND (`b!.c` AS c IS NULL)) AND (`b!.d` AS d IS NULL)) THEN NULL ELSE struct(`b!.c` AS c, `b!.d` AS d) END AS `b!`'>]

    :param node:
    :param separator:
    :param prefix:
    :return:
    """
    cols = []
    for key, value in node.items():
        if value is None:
            cols.append(f.col(quote(prefix + key)).alias(key))
        else:
            fields = _build_struct_from_tree(value, separator, prefix + key + separator)
            # We don't want structs where all fields are null so we check for this
            all_fields_are_null = f.lit(True)
            for field in fields:
                all_fields_are_null = all_fields_are_null & f.isnull(field)

            struct_col = f.when(all_fields_are_null, f.lit(None)).otherwise(f.struct(*fields)).alias(key)
            cols.append(struct_col)
    return cols


def unflatten(df: DataFrame, separator: str = ".") -> DataFrame:
    """Reverse of the flatten operation
    Nested fields names will be separated from each other using the specified separator

    Example:

    >>> spark = get_spark_session("doctest")
    >>> df = spark.createDataFrame([(1, 1, 1, 1)], "id INT, `s.a` INT, `s.b.c` INT, `s.b.d` INT")
    >>> df.printSchema()
    root
     |-- id: integer (nullable = true)
     |-- s.a: integer (nullable = true)
     |-- s.b.c: integer (nullable = true)
     |-- s.b.d: integer (nullable = true)
    <BLANKLINE>
    >>> unflatten(df).printSchema()
    root
     |-- id: integer (nullable = true)
     |-- s: struct (nullable = true)
     |    |-- a: integer (nullable = true)
     |    |-- b: struct (nullable = true)
     |    |    |-- c: integer (nullable = true)
     |    |    |-- d: integer (nullable = true)
    <BLANKLINE>
    >>> df = spark.createDataFrame([(1, 1, 1)], "id INT, `s.s1?a.a1` INT, `s.s1?b.b1` INT")
    >>> df.printSchema()
    root
     |-- id: integer (nullable = true)
     |-- s.s1?a.a1: integer (nullable = true)
     |-- s.s1?b.b1: integer (nullable = true)
    <BLANKLINE>
    >>> unflatten(df, "?").printSchema()
    root
     |-- id: integer (nullable = true)
     |-- s.s1: struct (nullable = true)
     |    |-- a.a1: integer (nullable = true)
     |    |-- b.b1: integer (nullable = true)
    <BLANKLINE>

    :param df: a DataFrame
    :param separator: It might be useful to change the separator when some DataFrame's column names already contain dots
    :return: a flattened DataFrame
    """
    # The idea is to recursively write a "SELECT struct(a, struct(s.b.c, s.b.d)) as s" for each nested column.
    # There is a little twist as we don't want to rebuild the struct if all its fields are null, so we add a CASE WHEN

    def has_structs(df: DataFrame) -> bool:
        struct_fields = [field for field in df.schema if isinstance(field.dataType, StructType)]
        return len(struct_fields) > 0

    if has_structs(df):
        df = flatten(df)

    tree = _build_nested_struct_tree(df.columns, separator)
    cols = _build_struct_from_tree(tree, separator)
    return df.select(cols)


def flatten_schema(schema: StructType, explode: bool) -> StructType:
    """Transform a DataFrame schema into a new schema where all structs have been flattened.
    The field names are kept, with a '.' separator for struct fields.
    If `explode` option is set, arrays are exploded with a '!' separator.

    Example:
    >>> spark = get_spark_session("doctest")
    >>> df = spark.createDataFrame(
    ...     [
    ...         (1, {"a": 1, "b": [{"c": 2, "d": 3}], "e": [4, 5]}),
    ...         (2, None),
    ...     ],
    ...     "id INT, s STRUCT<a:INT, b:ARRAY<STRUCT<c:INT, d:INT, e:ARRAY<INT>>>>"
    ... )
    >>> df.schema.simpleString()
    'struct<id:int,s:struct<a:int,b:array<struct<c:int,d:int,e:array<int>>>>>'
    >>> flatten_schema(df.schema, explode=True).simpleString()
    'struct<id:int,s.a:int,s.b!.c:int,s.b!.d:int,s.b!.e!:int>'
    >>> flatten_schema(df.schema, explode=False).simpleString()
    'struct<id:int,s.a:int,s.b:array<struct<c:int,d:int,e:array<int>>>>'

    :param schema: A Spark DataFrame's Schema
    :param explode: If set, arrays are exploded and a '!' separator is appended to their name.
    :return:
    """

    def flatten_data_type(
        prefix: str, data_type: DataType, is_nullable: bool, metadata: Dict[str, str]
    ) -> List[StructField]:
        if isinstance(data_type, StructType):
            return flatten_struct_type(data_type, is_nullable, prefix + ".")
        elif isinstance(data_type, ArrayType) and explode:
            return flatten_data_type(
                prefix + "!", data_type.elementType, is_nullable or data_type.containsNull, metadata
            )
        else:
            return [StructField(prefix, data_type, is_nullable, metadata)]

    def flatten_struct_type(schema: StructType, previous_nullable: bool = False, prefix: str = "") -> List[StructField]:
        res = []
        for field in schema:
            if isinstance(field.dataType, StructType):
                res += flatten_struct_type(
                    field.dataType, previous_nullable or field.nullable, prefix + field.name + "."
                )
            else:
                res += flatten_data_type(
                    prefix + field.name, field.dataType, previous_nullable or field.nullable, field.metadata
                )
        return res

    return StructType(flatten_struct_type(schema))


def structify(df: DataFrame, struct_name: str, primary_key: Union[str, Iterable[str]], deduplicate: bool) -> DataFrame:
    """Encapsulates all columns of the given dataframe inside a STRUCT.
    If 'deduplicate' is set to true, the dataframe will be grouped by primary_key
    and the STRUCT will be aggregated into an ARRAY<STRUCT>.

    :param df: a Dataframe
    :param struct_name: name of the struct that will encapsulate all the columns
    :param primary_key: name of the columns to keep out of the struct
    :param deduplicate: If set to true, the STRUCT will be aggregated into an ARRAY<STRUCT>.
    :return: a dataframe
    """
    if type(primary_key) == str:
        primary_key = [primary_key]
    other_columns = [col for col in df.columns if col not in primary_key]
    df2 = df.select(*primary_key, f.struct(*other_columns).alias(struct_name))
    if deduplicate:
        return df2.groupBy(*primary_key).agg(f.collect_list(struct_name).alias(struct_name))
    else:
        return df2


def get_sortable_columns(df: DataFrame) -> List[str]:
    """Get the list of all columns that are sortable in a Dataframe.
    For instance, a column of type Map<K, V> is not sortable.

    :param df:
    :return:
    """
    return [quote(type.name) for type in df.schema if not isinstance(type.dataType, MapType)]


def unpivot(df: DataFrame, pivot_columns: List[str], key_alias: str = "key", value_alias: str = "value") -> DataFrame:
    """Unpivot the given DataFrame along the specified pivot columns.
    All columns that are not pivot columns should have the same type.

    Example:

    >>> spark = get_spark_session("doctest")
    >>> df = spark.createDataFrame([
    ...    (2018, "Orange",  None, 4000, None),
    ...    (2018, "Beans",   None, 1500, 2000),
    ...    (2018, "Banana",  2000,  400, None),
    ...    (2018, "Carrots", 2000, 1200, None),
    ...    (2019, "Orange",  5000, None, 5000),
    ...    (2019, "Beans",   None, 1500, 2000),
    ...    (2019, "Banana",  None, 1400,  400),
    ...    (2019, "Carrots", None,  200, None),
    ...  ], "year INT, product STRING, Canada INT, China INT, Mexico INT"
    ... )
    >>> df.show()
    +----+-------+------+-----+------+
    |year|product|Canada|China|Mexico|
    +----+-------+------+-----+------+
    |2018| Orange|  null| 4000|  null|
    |2018|  Beans|  null| 1500|  2000|
    |2018| Banana|  2000|  400|  null|
    |2018|Carrots|  2000| 1200|  null|
    |2019| Orange|  5000| null|  5000|
    |2019|  Beans|  null| 1500|  2000|
    |2019| Banana|  null| 1400|   400|
    |2019|Carrots|  null|  200|  null|
    +----+-------+------+-----+------+
    <BLANKLINE>
    >>> unpivot(df, ['year', 'product'], key_alias='country', value_alias='total').show(100)
    +----+-------+-------+-----+
    |year|product|country|total|
    +----+-------+-------+-----+
    |2018| Orange| Canada| null|
    |2018| Orange|  China| 4000|
    |2018| Orange| Mexico| null|
    |2018|  Beans| Canada| null|
    |2018|  Beans|  China| 1500|
    |2018|  Beans| Mexico| 2000|
    |2018| Banana| Canada| 2000|
    |2018| Banana|  China|  400|
    |2018| Banana| Mexico| null|
    |2018|Carrots| Canada| 2000|
    |2018|Carrots|  China| 1200|
    |2018|Carrots| Mexico| null|
    |2019| Orange| Canada| 5000|
    |2019| Orange|  China| null|
    |2019| Orange| Mexico| 5000|
    |2019|  Beans| Canada| null|
    |2019|  Beans|  China| 1500|
    |2019|  Beans| Mexico| 2000|
    |2019| Banana| Canada| null|
    |2019| Banana|  China| 1400|
    |2019| Banana| Mexico|  400|
    |2019|Carrots| Canada| null|
    |2019|Carrots|  China|  200|
    |2019|Carrots| Mexico| null|
    +----+-------+-------+-----+
    <BLANKLINE>

    :param df: a DataFrame
    :param pivot_columns: The list of columns names on which to perform the pivot
    :param key_alias: alias given to the 'key' column
    :param value_alias: alias given to the 'value' column
    :return:
    """
    pivoted_columns = [(c, t) for (c, t) in df.dtypes if c not in pivot_columns]
    cols, types = zip(*pivoted_columns)

    # Check that all columns have the same type.
    assert_true(
        len(set(types)) == 1,
        ("All pivoted columns should be of the same type:\n Pivoted columns are: %s" % pivoted_columns),
    )

    # Create and explode an array of (column_name, column_value) structs
    kvs = f.explode(
        f.array(*[f.struct(f.lit(c).alias(key_alias), f.col(quote(c)).alias(value_alias)) for c in cols])
    ).alias("kvs")

    return df.select([f.col(c) for c in quote_columns(pivot_columns)] + [kvs]).select(
        quote_columns(pivot_columns) + ["kvs.*"]
    )


def union_by_name_sparse(left_df: DataFrame, right_df: DataFrame, use_flatten: bool = False) -> DataFrame:
    """Builds the union of two DataFrame but ignore columns that are from the other DataFrame

    :param left_df:
    :param right_df:
    :param use_flatten: If set to true, each input DataFrame will be flattened and the union unflattened
    :return:
    """
    if use_flatten:
        left_df = flatten(left_df)
        right_df = flatten(right_df)
    left_cols = set(left_df.columns)
    right_cols = set(right_df.columns)
    left_only_cols = left_cols.difference(right_cols)
    right_only_cols = right_cols.difference(left_cols)
    left_df = left_df.drop(*left_only_cols)
    right_df = right_df.drop(*right_only_cols)
    res_df = left_df.unionByName(right_df)
    if use_flatten:
        res_df = unflatten(res_df)
    return res_df


def union_data_frames(left_df: DataFrame, right_df: DataFrame, use_flatten: bool = False) -> DataFrame:
    """Union between two DataFrames, even if schemas are different

    :param left_df:
    :param right_df:
    :param use_flatten: If set to true, each input DataFrame will be flattened and the union unflattened
    :return:
    """

    def order_df_and_add_missing_cols(
        df: DataFrame, columns_order_list: List[str], df_missing_fields: Set[str]
    ) -> DataFrame:
        columns = []
        for name in columns_order_list:
            if name not in df_missing_fields:
                columns.append(f.col(quote(name)))
            else:
                columns.append(f.lit(None).alias(name))
        return df.select(columns)

    def order_and_union_data_frames(
        left_df: DataFrame, right_df: DataFrame, right_only_cols: Set[str], left_only_cols: Set[str]
    ) -> DataFrame:

        missing_columns = [f.lit(None).alias(col) for col in right_only_cols]

        left_df = left_df.select([f.col(c) for c in quote_columns(left_df.columns)] + missing_columns)
        right_df = order_df_and_add_missing_cols(right_df, left_df.columns, left_only_cols)
        return left_df.union(right_df)

    if use_flatten:
        left_df = flatten(left_df)
        right_df = flatten(right_df)

    if left_df is None:
        return right_df
    if right_df is None:
        return left_df
    if left_df.columns == right_df.columns:
        return left_df.union(right_df)

    left_df_col_list = set(left_df.columns)
    right_df_col_list = set(right_df.columns)
    left_only_cols = left_df_col_list.difference(right_df_col_list)
    right_only_cols = right_df_col_list.difference(left_df_col_list)
    res_df = order_and_union_data_frames(left_df, right_df, right_only_cols, left_only_cols)
    if use_flatten:
        res_df = unflatten(res_df)
    return res_df


def make_global_df(*dataframes: DataFrame, use_flatten: bool = False) -> DataFrame:
    """For spark 3.2 and beyond, you won't need this anymore, as unionByName does the same stuff:

    df = df1.unionByName(df2, allowMissingColumns=True)

    Example:

    >>> from pyspark import Row
    >>> spark = get_spark_session("doctest")
    >>> df1 = spark.createDataFrame(data=[
    ...   Row(a=1, b="1"),
    ...   Row(a=2, b="2"),
    ... ], schema="a INT, b STRING")
    >>> df2 = spark.createDataFrame(data=[
    ...   Row(a=3, c=True),
    ...   Row(a=4, c=False),
    ... ], schema="a INT, c BOOLEAN")
    >>> make_global_df(df1, df2, use_flatten=False).show()
    +---+----+-----+
    |  a|   b|    c|
    +---+----+-----+
    |  1|   1| null|
    |  2|   2| null|
    |  3|null| true|
    |  4|null|false|
    +---+----+-----+
    <BLANKLINE>

    :param dataframes: Dataframes to be merged into a big one.
    :param use_flatten: If True, split structs into columns while merging DataFrames
    :return: A big merged dataframe, with null values for df with missing columns.
    """
    global_df = None
    for df in dataframes:
        if global_df:
            global_df = union_data_frames(global_df, df, use_flatten=use_flatten)
        else:
            global_df = df
    return global_df


def coalesce_join(
    left_df: DataFrame, right_df: DataFrame, on: Column, how: str, invert_cols: Optional[List[str]] = None
) -> DataFrame:
    """Perform a join between two DataFrames and coalesces all the fields together,
     using fields from the right side when the ones from the left side are null.
     It can also perform an inverse coalesce on columns indicated in the variable invert_cols

    :param left_df: Left side of the join
    :param right_df: Right side of the join
    :param on: a string for the join column name, a list of column names,
        a join expression (Column), or a list of Columns.
        If `on` is a string or a list of strings indicating the name of the join column(s),
        the column(s) must exist on both sides, and this performs an equi-join.
    :param how: str, default ``inner``. Must be one of: ``inner``, ``cross``, ``outer``,
        ``full``, ``full_outer``, ``left``, ``left_outer``, ``right``, ``right_outer``,
        ``left_semi``, and ``left_anti``.
    :param invert_cols: list of column names on which an inverse coalesce is performed
    :return:
    """
    if invert_cols is None:
        invert_cols = []

    return left_df.join(right_df, on=on, how=how).select(
        [
            f.coalesce(right_df[quote(column)], left_df[quote(column)]).alias(column)
            if column in invert_cols
            else f.coalesce(left_df[quote(column)], right_df[quote(column)]).alias(column)
            for column in left_df.columns
        ]
    )


def sanitize_fields_name(
    spark: SparkSession, df: DataFrame, unwanted_characters: str = '-&"[].', replacement_character: str = "_"
) -> DataFrame:
    """Performs field names sanitization by removing unwanted character
    and adding an '_' if field name begin with a digit.

    Example:
    >>> from pyspark.sql import Row
    >>> spark = get_spark_session("doctest")
    >>> schema = StructType(
    ... [
    ...     StructField('a-b', StringType()),
    ...     StructField('c', StructType(
    ...         [
    ...         StructField('d-e', StringType()),
    ...         StructField('f', StringType())
    ...         ]
    ...         )
    ...     ),
    ...     StructField('g', MapType(StringType(),StringType())),
    ...     StructField('1h', StringType())
    ... ])
    >>> data= [ Row('a-b',('1','2'),{'h&i':'1','j-k':'l-m'},'3')]
    >>> df = spark.createDataFrame(data, schema)
    >>> df.printSchema()
    root
     |-- a-b: string (nullable = true)
     |-- c: struct (nullable = true)
     |    |-- d-e: string (nullable = true)
     |    |-- f: string (nullable = true)
     |-- g: map (nullable = true)
     |    |-- key: string
     |    |-- value: string (valueContainsNull = true)
     |-- 1h: string (nullable = true)
    <BLANKLINE>
    >>> sanitize_fields_name(spark, df).printSchema()
    root
     |-- a_b: string (nullable = true)
     |-- c: struct (nullable = true)
     |    |-- d_e: string (nullable = true)
     |    |-- f: string (nullable = true)
     |-- g: map (nullable = true)
     |    |-- key: string
     |    |-- value: string (valueContainsNull = true)
     |-- _1h: string (nullable = true)
    <BLANKLINE>
    >>> df.show(1,False)
    +---+------+----------------------+---+
    |a-b|c     |g                     |1h |
    +---+------+----------------------+---+
    |a-b|{1, 2}|{j-k -> l-m, h&i -> 1}|3  |
    +---+------+----------------------+---+
    <BLANKLINE>
    >>> sanitize_fields_name(spark, df).show(1,False)
    +---+------+----------------------+---+
    |a_b|c     |g                     |_1h|
    +---+------+----------------------+---+
    |a-b|{1, 2}|{j-k -> l-m, h&i -> 1}|3  |
    +---+------+----------------------+---+
    <BLANKLINE>

    :spark: sparkSession to use
    :df: Dataframe to sanitize
    :unwanted_characters: string composed of each character to be replaced
    :replacement_character: character used to replace unwanted character
    :return: sanitized Dataframe
    """

    # source : https://stackoverflow.com/questions/43004849/rename-nested-field-in-spark-dataframe

    def sanitize_field_name(s: str) -> str:
        new_name = s
        for char in unwanted_characters:
            new_name = new_name.replace(char, replacement_character)
        if new_name[0] in "0123456789":
            new_name = "_" + new_name
        return new_name

    def sanitize_field(field: StructField) -> StructField:
        field = copy(field)
        field.name = sanitize_field_name(field.name)
        field.dataType = clean_schema(field.dataType)
        return field

    def clean_schema(datatype: DataType) -> StructType:
        datatype = copy(datatype)
        # If the type is a StructType we need to recurse otherwise we can return since
        # we've reached the leaf node
        if isinstance(datatype, StructType):
            # We call our sanitizer for all top level fields
            datatype.fields = [sanitize_field(field) for field in datatype.fields]
        elif isinstance(datatype, ArrayType):
            datatype.elementType = clean_schema(datatype.elementType)
        return cast(StructType, datatype)

    return spark.createDataFrame(df.rdd, clean_schema(df.schema))


def ascending_forest_traversal(df: DataFrame, id: str, parent_id: str) -> DataFrame:
    """Given a DataFrame representing a labeled forest with columns "id", "parent_id" and other label columns,
    performs a graph traversal that will return a DataFrame with the same schema that gives for each node
    the labels of it's furthest ancestor.

    This algorithm is optimized for lowly-connected graphs that fit in RAM.
    In other words, for a graph G = (V, E) we assume that |E| << |V|

    It has a security against dependency cycles, but no security preventing
    a combinatorics explosion if some nodes have more than one parent.

    Example:

    >>> spark = get_spark_session("doctest")

    # Given a DataFrame with pokemon attributes and evolution links

    >>> df = spark.sql('''
    ...     SELECT
    ...       col1 as `pokemon.id`,
    ...       col2 as `pokemon.evolve_to_id`,
    ...       col3 as `pokemon.name`,
    ...       col4 as `pokemon.types`
    ...     FROM VALUES
    ...       (4, 5, 'Charmander', ARRAY('Fire')),
    ...       (5, 6, 'Charmeleon', ARRAY('Fire')),
    ...       (6, NULL, 'Charizard', ARRAY('Fire', 'Flying'))
    ... ''')
    >>> df.show()
    +----------+--------------------+------------+--------------+
    |pokemon.id|pokemon.evolve_to_id|pokemon.name| pokemon.types|
    +----------+--------------------+------------+--------------+
    |         4|                   5|  Charmander|        [Fire]|
    |         5|                   6|  Charmeleon|        [Fire]|
    |         6|                null|   Charizard|[Fire, Flying]|
    +----------+--------------------+------------+--------------+
    <BLANKLINE>

    # We compute a DataFrame that for each pokemon.id gives the attributes of its highest level of evolution

    >>> ascending_forest_traversal(df, "pokemon.id", "pokemon.evolve_to_id").orderBy("`pokemon.id`").show()
    +----------+--------------------+------------+--------------+
    |pokemon.id|pokemon.evolve_to_id|pokemon.name| pokemon.types|
    +----------+--------------------+------------+--------------+
    |         4|                null|   Charizard|[Fire, Flying]|
    |         5|                null|   Charizard|[Fire, Flying]|
    |         6|                null|   Charizard|[Fire, Flying]|
    +----------+--------------------+------------+--------------+
    <BLANKLINE>

    :param df: a Spark DataFrame
    :param id: name of the column that represent the node's ids
    :param parent_id: name of the column that represent the parent node's ids
    :return: a DataFrame with the same schema as the input DataFrame that gives
        for each node the labels of it's furthest ancestor
    """
    assert_true(id in df.columns, "Could not find column %s in dataframe's columns: %s" % (id, df.columns))
    assert_true(
        parent_id in df.columns, "Could not find column %s in dataframe's columns: %s" % (parent_id, df.columns)
    )
    df = df.repartition(200, f.col(quote(id))).persist()
    df_null = df.where(f.col(quote(parent_id)).isNull())
    df_not_null = df.where(f.col(quote(parent_id)).isNotNull()).persist()
    do_continue = True

    while do_continue:
        joined_df_not_null = (
            df_not_null.alias("a")
            .join(df.alias("b"), f.col("a." + quote(parent_id)) == f.col("b." + quote(id)), "left")
            .select(
                f.col("a." + quote(id)).alias(id),
                f.when(f.col("b." + quote(parent_id)) == f.col("a." + quote(id)), f.lit(None))
                .otherwise(f.col("b." + quote(parent_id)))
                .alias(parent_id),
                *[
                    f.coalesce(f.col("b." + quote(col)), f.col("a." + quote(col))).alias(col)
                    for col in df.columns
                    if col not in (id, parent_id)
                ],
            )
        )
        joined_df_not_null = joined_df_not_null.persist()
        new_df_not_null = joined_df_not_null.where(f.col(quote(parent_id)).isNotNull()).persist()
        do_continue = new_df_not_null.count() > 0
        new_df_null = df_null.union(joined_df_not_null.where(f.col(quote(parent_id)).isNull()))

        df_not_null = new_df_not_null
        df_null = new_df_null

    return df_null.union(df_not_null)


def empty_array(element_type: Union[DataType, str]) -> Column:
    """Creates an empty Spark array column of the specified type.
    This is a workaround because the Spark method typedLit is not available in PySpark

    :param element_type: the type of the array's element
    :return: a Spark Column representing an empty array.
    """
    return f.array_except(f.array(f.lit(None).cast(element_type)), f.array(f.lit(None)))


def to_generic_struct(*columns: str, col_name_alias: str = "name", col_value_alias: str = "value") -> Column:
    """Spark function that transforms a set of columns into an
    ARRAY<STRUCT<name: STRING, value: STRING> (column_name -> column_value)

    Example:

    >>> spark = get_spark_session("doctest")
    >>> df = spark.sql('''
    ...     SELECT
    ...       col1 as `pokemon.id`,
    ...       col2 as `pokemon.name`,
    ...       col3 as `pokemon.types`
    ...     FROM VALUES
    ...       (4, 'Charmander', ARRAY(NAMED_STRUCT('type', 'Fire'))),
    ...       (5, 'Charmeleon', ARRAY(NAMED_STRUCT('type', 'Fire'))),
    ...       (6, 'Charizard',  ARRAY(NAMED_STRUCT('type', 'Fire'), NAMED_STRUCT('type', 'Flying')))
    ... ''')
    >>> df.show()
    +----------+------------+------------------+
    |pokemon.id|pokemon.name|     pokemon.types|
    +----------+------------+------------------+
    |         4|  Charmander|          [{Fire}]|
    |         5|  Charmeleon|          [{Fire}]|
    |         6|   Charizard|[{Fire}, {Flying}]|
    +----------+------------+------------------+
    <BLANKLINE>
    >>> res = df.select(to_generic_struct("pokemon.id", "pokemon.name", "pokemon.types").alias('values'))
    >>> res.printSchema()
    root
     |-- values: array (nullable = false)
     |    |-- element: struct (containsNull = false)
     |    |    |-- name: string (nullable = false)
     |    |    |-- value: string (nullable = false)
    <BLANKLINE>
    >>> res.show(10, False)
    +---------------------------------------------------------------------------------+
    |values                                                                           |
    +---------------------------------------------------------------------------------+
    |[{pokemon.id, 4}, {pokemon.name, Charmander}, {pokemon.types, [{Fire}]}]         |
    |[{pokemon.id, 5}, {pokemon.name, Charmeleon}, {pokemon.types, [{Fire}]}]         |
    |[{pokemon.id, 6}, {pokemon.name, Charizard}, {pokemon.types, [{Fire}, {Flying}]}]|
    +---------------------------------------------------------------------------------+
    <BLANKLINE>

    :param columns:
    :param col_name_alias: (Default="name") alias of the field containing the column names in the returned struct
    :param col_value_alias: (Default="value") alias of the field containing the column values in the returned struct
    :return:
    """
    return f.array(
        *[
            f.struct(f.lit(c).alias(col_name_alias), f.col(quote(c)).astype(StringType()).alias(col_value_alias))
            for c in columns
        ]
    )


def nullable(col: Column) -> Column:
    """Spark function that makes a DataFrame column nullable.
    This is especially useful for literal which are always non-nullable by default.

    Example:

    >>> spark = get_spark_session("doctest")
    >>> from pyspark.sql import functions as f
    >>> df = spark.sql('''SELECT 1 as a''').withColumn("b", f.lit("2"))
    >>> df.printSchema()
    root
     |-- a: integer (nullable = false)
     |-- b: string (nullable = false)
    <BLANKLINE>
    >>> res = df.withColumn('a', nullable(f.col('a'))).withColumn('b', nullable(f.col('b')))
    >>> res.printSchema()
    root
     |-- a: integer (nullable = true)
     |-- b: string (nullable = true)
    <BLANKLINE>

    :param col:
    :return:
    """
    return f.when(~col.isNull(), col)


__split_col_name_regex = re.compile(
    r"""(                   # Matching group 1 [Everything that comes before the first DOT split]
            (?:                 # Non-matching group
                    [^`.]*          # Everything but BACKQUOTE and DOT zero or more times
                    `               # BACKQUOTE
                    [^`]*           # Everything but BACKQUOTE zero or more times [DOTS are allowed between BACKQUOTES]
                    `               # BACKQUOTE
                |               # OR
                    [^`.]+          # Everything but BACKQUOTE and DOT one or more times
            )+                  # Repeat this group one or more times
        )
        ([.]|$)             # Matching group 2 [The separator: DOT or end of line (to match strings without dots)]
    """,
    re.VERBOSE,  # Allows to add comments inside the regex
)

re.compile(r"([^.]+)([.]|$)").findall("a.b.c")


def split_col_name(col_name: str) -> List[str]:
    """Splits a Spark column name representing a nested field into a list of path parts.

    Examples:
    In this example: `a` is a struct containing a field `b`.

    >>> split_col_name("a.b")
    ['a', 'b']
    >>> split_col_name("ab")
    ['ab']

    Field names can contain dots when escaped between backquotes (`)

    >>> split_col_name("`a.b`")
    ['a.b']
    >>> split_col_name("`a.b`.`c.d`")
    ['a.b', 'c.d']
    >>> split_col_name("`a.b`.c.`d`")
    ['a.b', 'c', 'd']

    Field names can even contain backquotes, by escaping backquotes with another backquote.

    >>> split_col_name("`a``b`")
    ['a`b']
    >>> split_col_name("`.a.``.b.`")
    ['.a.`.b.']
    >>> split_col_name("`ab`.`c``d`.fg")
    ['ab', 'c`d', 'fg']

    :param col_name:
    :return:
    """
    col_parts = [unquote(match[0]) for match in __split_col_name_regex.findall(col_name)]
    return col_parts


def get_col_type(schema: StructType, col_name: str) -> DataType:
    """Fetch recursively the DataType of a column inside a DataFrame schema (or more generally any StructType)

    Example:
    >>> spark = get_spark_session("doctest")
    >>> df = spark.createDataFrame([
    ...      (1, {"a.b" : {"c.d": 1, "e": "1", "f`g": True}}),
    ...      (2, {"a.b" : {"c.d": 2, "e": "2", "f`g": True}}),
    ...      (3, {"a.b" : {"c.d": 3, "e": "3", "f`g": True}}),
    ...   ],
    ...   "id INT, `a.b` STRUCT<`c.d`:INT, e:STRING, `f``g`:BOOLEAN>"
    ... )
    >>> get_col_type(df.schema, "`a.b`.`c.d`").simpleString()
    'int'
    >>> get_col_type(df.schema, "`a.b`.e").simpleString()
    'string'
    >>> get_col_type(df.schema, "`a.b`.`f``g`").simpleString()
    'boolean'

    :param schema: the DataFrame schema (or StructField) in which the column type will be fetched.
    :param col_name: the name of the column to get
    :return:
    """
    col_parts = split_col_name(col_name)

    def get_col(col: str, fields: List[StructField]) -> StructField:
        for field in fields:
            if field.name == col:
                return field
        raise ValueError(f'Cannot resolve column name "{col_name}"')

    struct: Union[StructType, DataType] = schema
    for col_part in col_parts:
        assert_true(isinstance(struct, StructType))
        struct = cast(StructType, struct)
        struct = get_col(col_part, struct.fields).dataType
    assert_true(isinstance(struct, DataType))
    return cast(DataType, struct)


def to_generic_typed_struct(df: DataFrame, col_names: List[str]) -> DataFrame:
    """Transforms the specified struct columns of a Dataframe into generic typed struct columns with the following
    generic schema (based on https://spark.apache.org/docs/2.4.5/sql-reference.html#data-types):

    .. code-block::

        STRUCT<
            key: STRING, -- (name of the field inside the struct)
            type: STRING, -- (type of the field inside the struct)
            value: STRUCT< -- (all the fields will be null except for the one with the correct type)
                date: DATE,
                timestamp: TIMESTAMP,
                int: LONG,
                float: DOUBLE,
                boolean: BOOLEAN,
                string: STRING,
                bytes: BINARY
            >
        >

    The following spark types will be automatically cast into the more generic following types:

    - tinyint, smallint, int -> bigint
    - float, decimal -> double

    Limitations
    -----------
    Currently, complex field types (structs, maps, arrays) are not supported.
    All fields of the struct columns to convert must be of basic types.

    Examples
    --------

    >>> spark = get_spark_session("doctest")
    >>> df = spark.createDataFrame(
    ...     [(1, {"first.name": "Jacques", "age": 25, "is.an.adult": True}),
    ...      (2, {"first.name": "Michel", "age": 12, "is.an.adult": False}),
    ...      (3, {"first.name": "Marie", "age": 36, "is.an.adult": True})],
    ...     "id INT, `person.struct` STRUCT<`first.name`:STRING, age:INT, `is.an.adult`:BOOLEAN>"
    ... )
    >>> df.show(truncate=False)
    +---+-------------------+
    |id |person.struct      |
    +---+-------------------+
    |1  |{Jacques, 25, true}|
    |2  |{Michel, 12, false}|
    |3  |{Marie, 36, true}  |
    +---+-------------------+
    <BLANKLINE>
    >>> res = to_generic_typed_struct(df, ["`person.struct`"])
    >>> res.printSchema()
    root
     |-- id: integer (nullable = true)
     |-- person.struct: array (nullable = false)
     |    |-- element: struct (containsNull = false)
     |    |    |-- key: string (nullable = false)
     |    |    |-- type: string (nullable = false)
     |    |    |-- value: struct (nullable = false)
     |    |    |    |-- boolean: boolean (nullable = true)
     |    |    |    |-- bytes: binary (nullable = true)
     |    |    |    |-- date: date (nullable = true)
     |    |    |    |-- float: double (nullable = true)
     |    |    |    |-- int: long (nullable = true)
     |    |    |    |-- string: string (nullable = true)
     |    |    |    |-- timestamp: timestamp (nullable = true)
    <BLANKLINE>
    >>> res.show(10, False) # noqa: E501
    +---+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |id |person.struct                                                                                                                                                                                  |
    +---+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |1  |[{first.name, string, {null, null, null, null, null, Jacques, null}}, {age, int, {null, null, null, null, 25, null, null}}, {is.an.adult, boolean, {true, null, null, null, null, null, null}}]|
    |2  |[{first.name, string, {null, null, null, null, null, Michel, null}}, {age, int, {null, null, null, null, 12, null, null}}, {is.an.adult, boolean, {false, null, null, null, null, null, null}}]|
    |3  |[{first.name, string, {null, null, null, null, null, Marie, null}}, {age, int, {null, null, null, null, 36, null, null}}, {is.an.adult, boolean, {true, null, null, null, null, null, null}}]  |
    +---+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    <BLANKLINE>

    :param df: Dataframe to transform
    :param col_names: List of column names to transform
    :return: a Dataframe with fields transformed into struct with Data type name
    """

    source_to_cast = {
        "date": "date",
        "timestamp": "timestamp",
        "tinyint": "bigint",
        "smallint": "bigint",
        "int": "bigint",
        "bigint": "bigint",
        "float": "double",
        "double": "double",
        "boolean": "boolean",
        "string": "string",
        "binary": "binary",
    }
    """Mapping indicating for each source Spark DataTypes the type into which it will be cast."""

    cast_to_name = {
        "binary": "bytes",
        "bigint": "int",
        "double": "float",
    }
    """Mapping indicating for each already cast Spark DataTypes the name of the corresponding field.
    When missing, the same name will be kept."""

    name_cast = {cast_to_name.get(value, value): value for value in source_to_cast.values()}
    # We make sure the types are sorted
    name_cast = {k: v for k, v in sorted(name_cast.items())}

    def match_regex_types(source_type: str) -> Optional[str]:
        """Matches the source types against regexes to identify more complex types (like Decimal(x, y))"""
        regex_to_cast_types = [(re.compile("decimal(.*)"), "float")]
        for regex, cast_type in regex_to_cast_types:
            if regex.match(source_type) is not None:
                return cast_type
        return None

    def field_to_col(field: StructField, column_name: str) -> Optional[Column]:
        """Transforms the specified field into a generic column"""
        source_type = field.dataType.simpleString()
        cast_type = source_to_cast.get(source_type)
        field_name = column_name + "." + quote(field.name)
        if cast_type is None:
            cast_type = match_regex_types(source_type)
        if cast_type is None:
            print(
                "WARNING: The field {field_name} is of type {source_type} which is currently unsupported. "
                "This field will be dropped.".format(field_name=field_name, source_type=source_type)
            )
            return None
        name_type = cast_to_name.get(cast_type, cast_type)
        return f.struct(
            f.lit(field.name).alias("key"),
            f.lit(name_type).alias("type"),
            # In the code below, we use f.expr instead of f.col because it looks like f.col
            # does not support column names with backquotes in them, but f.expr does :-p
            f.struct(
                *[
                    (f.expr(field_name) if name_type == name_t else f.lit(None)).astype(cast_t).alias(name_t)
                    for name_t, cast_t in name_cast.items()
                ]
            ).alias("value"),
        )

    for col_name in col_names:
        schema = get_col_type(df.schema, col_name)
        assert_true(isinstance(schema, StructType))
        columns = [field_to_col(field, col_name) for field in schema.fields]
        columns_2 = [col for col in columns if col is not None]
        df = df.withColumn(unquote(col_name), f.array(*columns_2).alias("values"))
    return df


def find_wider_type_for_two(t1: DataType, t2: DataType) -> Optional[str]:
    """Python wrapper for Spark's TypeCoercion.find_wider_type_for_two:

    Looking for a widened data type of two given data types with some acceptable loss of precision.
    E.g. there is no common type for double and decimal because double's range
    is larger than decimal, and yet decimal is more precise than double, but in
    union we would cast the decimal into double.

    WARNINGS:

    - the result is a simpleString
    - A SparkSession must already be instantiated for this method to work

    >>> from pyspark.sql.types import DecimalType, LongType, DoubleType, IntegerType
    >>> spark = get_spark_session("doctest")
    >>> find_wider_type_for_two(DecimalType(15, 5), DecimalType(15, 6))
    'decimal(16,6)'
    >>> find_wider_type_for_two(DecimalType(15, 5), DoubleType())
    'double'
    >>> find_wider_type_for_two(LongType(), IntegerType())
    'bigint'
    >>> find_wider_type_for_two(ArrayType(IntegerType()), IntegerType())

    :param t1:
    :param t2:
    :return:
    """
    from pyspark.sql import SparkSession

    spark = SparkSession._instantiatedSession
    sc = spark.sparkContext

    def _to_java_type(t: DataType):
        return spark._jsparkSession.parseDataType(t.json())

    jt1 = _to_java_type(t1)
    jt2 = _to_java_type(t2)
    j_type_coercion = getattr(getattr(sc._jvm.org.apache.spark.sql.catalyst.analysis, "TypeCoercion$"), "MODULE$")
    wider_type = j_type_coercion.findWiderTypeForTwo(jt1, jt2)
    if wider_type.nonEmpty():
        return wider_type.get().simpleString()
    else:
        return None


def get_common_columns(left_schema: StructType, right_schema: StructType) -> List[Column]:
    """Return a list of common Columns between two DataFrame schemas, casting them into the widest common type
    when required.

    When columns have incompatible types, they are simply not cast.

    >>> spark = get_spark_session("doctest")
    >>> df1 = spark.sql('''SELECT 'A' as id, CAST(1 as BIGINT) as d, 'a' as a''')
    >>> df2 = spark.sql('''SELECT 'A' as id, CAST(1 as DOUBLE) as d, ARRAY('a') as a''')
    >>> common_cols = get_common_columns(df1.schema, df2.schema)
    >>> df1.select(common_cols).printSchema()
    root
     |-- id: string (nullable = false)
     |-- d: double (nullable = false)
     |-- a: string (nullable = false)
    <BLANKLINE>
    >>> df2.select(common_cols).printSchema()
    root
     |-- id: string (nullable = false)
     |-- d: double (nullable = false)
     |-- a: array (nullable = false)
     |    |-- element: string (containsNull = false)
    <BLANKLINE>

    :param left_schema:
    :param right_schema:
    :return:
    """
    left_fields = {field.name: field for field in left_schema.fields}
    right_fields = {field.name: field for field in right_schema.fields}

    def get_columns() -> Generator[Column, None, None]:
        for name, left_field in left_fields.items():
            if name in right_fields:
                right_field: StructField = right_fields[name]
                if right_field.dataType == left_field.dataType:
                    common_type = None
                else:
                    common_type = find_wider_type_for_two(left_field.dataType, right_field.dataType)
                if common_type is not None:
                    yield f.col(quote(name)).astype(common_type)
                else:
                    yield f.col(quote(name))

    return list(get_columns())


def multi_join_with_common_key(
    dataframes: List[Tuple[DataFrame, str]], common_keys: Iterable[str], priority_list: List[str]
) -> DataFrame:
    """
    given a list of (dataframe, join_type) and a Tuple of common keys join all those dataframes
    >>> spark = get_spark_session("doctest")
    >>> df1 = spark.sql('''SELECT 'A' as id, ARRAY('a') as a''').cache()
    >>> df2 = spark.sql('''SELECT 'A' as id, CAST(1 as BIGINT) as b''').cache()
    >>> df3 = spark.sql('''SELECT 'A' as id, CAST(5 as DOUBLE) as c''').cache()
    >>> dfs = [(df1, "left_outer"), (df2, "full_outer"), (df3, "left_outer")]
    >>> join_priority_list = ["full_outer", "left_outer"]
    >>> result = multi_join_with_common_key(dfs, ('id',), join_priority_list)
    >>> result.show(truncate=False)
    +---+---+---+---+
    |id |a  |b  |c  |
    +---+---+---+---+
    |A  |[a]|1  |5.0|
    +---+---+---+---+
    <BLANKLINE>
    """
    common_keys = list(common_keys)

    dataframes.sort(key=lambda x: priority_list.index(x[1]))

    res = dataframes[0][0]
    for df, join_type in dataframes[1:]:
        res = res.join(df, common_keys, join_type)

    # But here, we want to have the structs sorted in alphanumeric ordering so we sort them accordingly
    columns = res.drop(*common_keys).columns
    columns.sort()

    return res.select(*common_keys, *columns)


def concat_all_columns(df: DataFrame, result_col_name: str, separator: str) -> DataFrame:
    """
    given a dataframe this function concat all columns and return a dataframe with all columns concatened

    >>> spark = get_spark_session("doctest")
    >>> df1 = spark.sql('''SELECT 'A' as id, 'a' as a''').cache()
    >>> result = concat_all_columns(df1, 'tt', ',')
    >>> result.show(truncate=False)
    +---+
    |tt |
    +---+
    |A,a|
    +---+
    <BLANKLINE>
    """
    return df.select(f.expr("concat_ws('%s',*)" % separator).alias(result_col_name))


def camel_case_fields_to_snake_case(df: DataFrame) -> DataFrame:
    """Given a dataframe this function concat all columns and return a dataframe with all columns concatened

    >>> spark = get_spark_session("doctest")
    >>> df = spark.createDataFrame(
    ...        [(1, 'a', [1, 2], {'a': 1, 'b': 2}, (1, [1, 2], {'a': 1, 'b': 2}))],
    ...        "intCol INT, StringCol STRING, arrayCol ARRAY<INT>, MapCol MAP<STRING, INT>, "
    ...        "structCol STRUCT<a: INT, InnerArrayCol: ARRAY<INT>, InnerMapCol: MAP<STRING, INT>>"
    ...    )
    >>> df.show(1, False)
    +------+---------+--------+----------------+-----------------------------+
    |intCol|StringCol|arrayCol|MapCol          |structCol                    |
    +------+---------+--------+----------------+-----------------------------+
    |1     |a        |[1, 2]  |{a -> 1, b -> 2}|{1, [1, 2], {a -> 1, b -> 2}}|
    +------+---------+--------+----------------+-----------------------------+
    <BLANKLINE>
    >>> df.printSchema()
    root
     |-- intCol: integer (nullable = true)
     |-- StringCol: string (nullable = true)
     |-- arrayCol: array (nullable = true)
     |    |-- element: integer (containsNull = true)
     |-- MapCol: map (nullable = true)
     |    |-- key: string
     |    |-- value: integer (valueContainsNull = true)
     |-- structCol: struct (nullable = true)
     |    |-- a: integer (nullable = true)
     |    |-- InnerArrayCol: array (nullable = true)
     |    |    |-- element: integer (containsNull = true)
     |    |-- InnerMapCol: map (nullable = true)
     |    |    |-- key: string
     |    |    |-- value: integer (valueContainsNull = true)
    <BLANKLINE>
    >>> result = camel_case_fields_to_snake_case(df)
    >>> result.show(1, False)
    +-------+----------+---------+----------------+-----------------------------+
    |int_col|string_col|array_col|map_col         |struct_col                   |
    +-------+----------+---------+----------------+-----------------------------+
    |1      |a         |[1, 2]   |{a -> 1, b -> 2}|{1, [1, 2], {a -> 1, b -> 2}}|
    +-------+----------+---------+----------------+-----------------------------+
    <BLANKLINE>
    >>> result.printSchema()
    root
     |-- int_col: integer (nullable = true)
     |-- string_col: string (nullable = true)
     |-- array_col: array (nullable = true)
     |    |-- element: integer (containsNull = true)
     |-- map_col: map (nullable = true)
     |    |-- key: string
     |    |-- value: integer (valueContainsNull = true)
     |-- struct_col: struct (nullable = true)
     |    |-- a: integer (nullable = true)
     |    |-- inner_array_col: array (nullable = true)
     |    |    |-- element: integer (containsNull = true)
     |    |-- inner_map_col: map (nullable = true)
     |    |    |-- key: string
     |    |    |-- value: integer (valueContainsNull = true)
    <BLANKLINE>
    """
    import re

    df = flatten(df)

    def camel_case_to_snake_case(s: str) -> str:
        return re.sub(r"(?<!^)(?<!\.)(?=[A-Z])", "_", s).lower()

    for field in df.schema:
        if isinstance(field.dataType, ArrayType) or isinstance(field.dataType, MapType):
            complex_field_mapping_str = field.simpleString()
            (field_name, field_mapping) = complex_field_mapping_str.split(":", 1)
            df = (
                df.selectExpr(
                    "*", f"CAST(`{field_name}` AS {camel_case_to_snake_case(field_mapping)}) AS tmp_snake_field"
                )
                .withColumn(field_name, f.col("tmp_snake_field"))
                .drop("tmp_snake_field")
            )
        df = df.withColumnRenamed(field.name, camel_case_to_snake_case(field.name))
    df = unflatten(df)
    return df


def remove_columns(df: DataFrame, struct_fields: List[str]) -> DataFrame:
    """Given a dataframe this function remove all columns (or nested element) given in argument with absolute
    name path (based on flatten function)

    >>> spark = get_spark_session("doctest")
    >>> df = spark.createDataFrame(
    ...        [(1, 'a', [1, 2], {'a': 1, 'b': 2}, (1, [1, 2], {'a': 1, 'b': 2}, (1, 2)))],
    ...        "id INT, col1 STRING, col2 ARRAY<INT>, col3 MAP<STRING, INT>, "
    ...        "col4 STRUCT<sub_col1: INT, sub_col2: ARRAY<INT>, sub_col3: MAP<STRING, INT>,"
    ...        "sub_col4: STRUCT<sub_col5 :INT, sub_col6 :INT>>"
    ...    )
    >>> df.show(1, False)
    +---+----+------+----------------+-------------------------------------+
    |id |col1|col2  |col3            |col4                                 |
    +---+----+------+----------------+-------------------------------------+
    |1  |a   |[1, 2]|{a -> 1, b -> 2}|{1, [1, 2], {a -> 1, b -> 2}, {1, 2}}|
    +---+----+------+----------------+-------------------------------------+
    <BLANKLINE>
    >>> df.printSchema()
    root
     |-- id: integer (nullable = true)
     |-- col1: string (nullable = true)
     |-- col2: array (nullable = true)
     |    |-- element: integer (containsNull = true)
     |-- col3: map (nullable = true)
     |    |-- key: string
     |    |-- value: integer (valueContainsNull = true)
     |-- col4: struct (nullable = true)
     |    |-- sub_col1: integer (nullable = true)
     |    |-- sub_col2: array (nullable = true)
     |    |    |-- element: integer (containsNull = true)
     |    |-- sub_col3: map (nullable = true)
     |    |    |-- key: string
     |    |    |-- value: integer (valueContainsNull = true)
     |    |-- sub_col4: struct (nullable = true)
     |    |    |-- sub_col5: integer (nullable = true)
     |    |    |-- sub_col6: integer (nullable = true)
    <BLANKLINE>
    >>> result = remove_columns(df, ['col1', 'col4.sub_col2', 'col4.sub_col4.sub_col6'])
    >>> result.show(1, False)
    +---+------+----------------+--------------------------+
    |id |col2  |col3            |col4                      |
    +---+------+----------------+--------------------------+
    |1  |[1, 2]|{a -> 1, b -> 2}|{1, {a -> 1, b -> 2}, {1}}|
    +---+------+----------------+--------------------------+
    <BLANKLINE>
    >>> result.printSchema()
    root
     |-- id: integer (nullable = true)
     |-- col2: array (nullable = true)
     |    |-- element: integer (containsNull = true)
     |-- col3: map (nullable = true)
     |    |-- key: string
     |    |-- value: integer (valueContainsNull = true)
     |-- col4: struct (nullable = true)
     |    |-- sub_col1: integer (nullable = true)
     |    |-- sub_col3: map (nullable = true)
     |    |    |-- key: string
     |    |    |-- value: integer (valueContainsNull = true)
     |    |-- sub_col4: struct (nullable = true)
     |    |    |-- sub_col5: integer (nullable = true)
    <BLANKLINE>
    """
    for struct_field in struct_fields:
        if "." in struct_field:
            col = struct_field.split(".")[0]
            struct = ".".join(struct_field.split(".")[1:])
            df = df.withColumn(col, f.col(col).dropFields(struct))
        else:
            df = df.drop(*struct_fields)
    return df


def left_join_dataframes_on_nullable_key(left_df: DataFrame, key: str, *dataframes: DataFrame) -> DataFrame:
    """
    This function avoid skews when joining datasets on a nullable key.
    """
    left_with_null_key = left_df.where("%s is null" % key)
    left_with_not_null_key = left_df.where("%s is not null" % key)

    left_with_right_fields = left_with_not_null_key
    for df in dataframes:
        left_with_right_fields = left_with_right_fields.join(df, key, "left")

    return union_data_frames(left_with_right_fields, left_with_null_key)


def convert_column_type(df: DataFrame, input_type: str, converted_type: str) -> DataFrame:
    """
    Cast all the columns of type input_type to converted_type of a dataframe
    """
    from pyspark.sql.functions import col

    for column in df.dtypes:
        col_name, col_type = column[0], column[1]
        if col_type == input_type:
            df = df.withColumn(col_name, col(col_name).cast(converted_type))
    return df
