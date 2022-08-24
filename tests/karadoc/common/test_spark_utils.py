from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from karadoc.common import spark_utils
from karadoc.spark.utils import get_spark_session
from karadoc.test_utils import pyspark_test_class

generic_typed_struct_type = """
ARRAY<STRUCT<
    key:STRING,
    type:STRING,
    value:STRUCT<
        boolean:BOOLEAN,
        bytes:BINARY,
        date:DATE,
        float:DOUBLE,
        int:BIGINT,
        string:STRING,
        timestamp:TIMESTAMP
    >
>>
"""


class TestSparkUtils(pyspark_test_class.PySparkTest):
    def test_extract_json_column(self):
        df = self.spark.createDataFrame(
            [
                (1, '{"a": 1, "b": 1}', '{"c": 1, "d": 1}'),
                (2, '{"a": 2, "b": 2}', '{"c": 2, "d": 2}'),
                (3, '{"a": 3, "b": 3}', '{"c": 3, "d": 3}'),
            ],
            "id INT, json1 STRING, json2 STRING",
        )
        df2 = spark_utils.extract_json_column(self.spark, df, ["json1", "json2"])

        json1_type = [col[1] for col in df2.dtypes if col[0] == "json1"]

        self.assertEqual(json1_type[0], "struct<a:bigint,b:bigint>")

        expected = self.spark.createDataFrame(
            [
                (1, {"a": 1, "b": 1}, {"c": 1, "d": 1}),
                (2, {"a": 2, "b": 2}, {"c": 2, "d": 2}),
                (3, {"a": 3, "b": 3}, {"c": 3, "d": 3}),
            ],
            "id INT, json1 STRUCT<a: BIGINT, b: BIGINT>, json2 STRUCT<c: BIGINT, d:BIGINT>",
        )

        self.assertEqual(df2.sort("id").collect(), expected.sort("id").collect())

    def test_extract_json_column_with_arrays(self):
        """We check that it works with arrays too"""
        df = self.spark.createDataFrame(
            [
                (1, '[{"a": 1}, {"a": 2}]', '[{"c": 1}, {"c": 2}]'),
                (2, '[{"a": 2}, {"a": 4}]', '[{"c": 2}, {"c": 4}]'),
                (3, '[{"a": 3}, {"a": 6}]', '[{"c": 3}, {"c": 6}]'),
            ],
            "id INT, json1 STRING, json2 STRING",
        )
        df2 = spark_utils.extract_json_column(self.spark, df, ["json1", "json2"])

        json1_type = [col[1] for col in df2.dtypes if col[0] == "json1"]

        self.assertEqual(json1_type[0], "array<struct<a:bigint>>")

        expected = self.spark.createDataFrame(
            [
                (1, [{"a": 1}, {"a": 2}], [{"c": 1}, {"c": 2}]),
                (2, [{"a": 2}, {"a": 4}], [{"c": 2}, {"c": 4}]),
                (3, [{"a": 3}, {"a": 6}], [{"c": 3}, {"c": 6}]),
            ],
            "id INT, json1 ARRAY<STRUCT<a: BIGINT>>, json2 ARRAY<STRUCT<c: BIGINT>>",
        )

        self.assertEqual(df2.sort("id").collect(), expected.sort("id").collect())

    def test_extract_json_column_with_nulls(self):
        """We check that it works with arrays too"""
        # fmt: off
        df = self.spark.createDataFrame(
            [(1, '[{"a": 1}, {"a": 2}]'),
             (2, None),
             (3, '[{"a": 3}, {"a": 6}]')],
            "id INT, json1 STRING"
        )
        df2 = spark_utils.extract_json_column(self.spark, df, ["json1"])

        expected = self.spark.createDataFrame(
            [(1, [{"a": 1}, {"a": 2}]),
             (2, None),
             (3, [{"a": 3}, {"a": 6}])],
            "id INT, json1 ARRAY<STRUCT<a: BIGINT>>"
        )
        # fmt: on

        self.assertEqual(df2.sort("id").collect(), expected.sort("id").collect())

    def test_extract_json_column_with_empty_array(self):
        """We check that it works with arrays too"""
        # fmt: off
        df = self.spark.createDataFrame(
            [(1, '[{"a": 1}, {"a": 2}]'),
             (2, "[]"),
             (3, '[{"a": 3}, {"a": 6}]')],
            "id INT, json1 STRING"
        )
        df2 = spark_utils.extract_json_column(self.spark, df, ["json1"])

        expected = self.spark.createDataFrame(
            [(1, [{"a": 1}, {"a": 2}]),
             (2, []),
             (3, [{"a": 3}, {"a": 6}])],
            "id INT, json1 ARRAY<STRUCT<a: BIGINT>>"
        )
        # fmt: on

        self.assertEqual(df2.sort("id").collect(), expected.sort("id").collect())

    def test_flatten_struct_in_df(self):
        df = self.spark.createDataFrame(
            [
                (1, {"a": 1, "b": 1}, {"c": 1, "d": 1}),
                (2, {"a": 2, "b": 2}, {"c": 2, "d": 2}),
                (3, {"a": 3, "b": 3}, {"c": 3, "d": 3}),
            ],
            StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField(
                        "json1",
                        StructType([StructField("a", LongType(), True), StructField("b", LongType(), True)]),
                        False,
                    ),
                    StructField(
                        "json2",
                        StructType([StructField("c", LongType(), True), StructField("d", LongType(), True)]),
                        False,
                    ),
                ]
            ),
        )
        df2 = spark_utils.flatten_struct_in_df(df, 1)

        self.assertEqual(str(df2.columns), "['id', 'json1_a', 'json1_b', 'json2_c', 'json2_d']")

    def test_flatten(self):
        # fmt: off
        df = self.spark.createDataFrame(
            [(1, {"a": 1, "b": {"c": 2, "d": 3}}),
             (2, None),
             (3, {"a": 1, "b": {"c": 2, "d": 3}})],
            "id INT, s STRUCT<a:INT, b:STRUCT<c:INT, d:INT>>",
        )
        # fmt: on
        df2 = spark_utils.flatten(df)

        # fmt: off
        expected = self.spark.createDataFrame(
            [(1, 1, 2, 3),
             (2, None, None, None),
             (3, 1, 2, 3)],
            "id INT, `s.a` INT, `s.b.c` INT, `s.b.d` INT"
        )
        # fmt: on
        self.assertEqual(df2.sort("id").collect(), expected.sort("id").collect())

    def test_flatten_with_complex_names(self):
        df = self.spark.createDataFrame(
            [
                (1, {"a.a1": 1, "b.b1": {"c.c1": 2, "d.d1": 3}}),
                (2, None),
                (3, {"a.a1": 1, "b.b1": {"c.c1": 2, "d.d1": 3}}),
            ],
            "`id.id1` INT, `s.s1` STRUCT<`a.a1`:INT, `b.b1`:STRUCT<`c.c1`:INT, `d.d1`:INT>>",
        )
        df2 = spark_utils.flatten(df, "?")

        # fmt: off
        expected = self.spark.createDataFrame(
            [(1, 1, 2, 3),
             (2, None, None, None),
             (3, 1, 2, 3)],
            "`id.id1` INT, `s.s1?a.a1` INT, `s.s1?b.b1?c.c1` INT, `s.s1?b.b1?d.d1` INT",
        )
        # fmt: on
        self.assertEqual(df2.sort("`id.id1`").collect(), expected.sort("`id.id1`").collect())

    def test_unflatten(self):
        # fmt: off
        df = self.spark.createDataFrame(
            [(1, 1, 2, 3),
             (2, None, None, None),
             (3, 1, 2, 3)],
            "id INT, `s.a` INT, `s.b.c` INT, `s.b.d` INT"
        )

        df2 = spark_utils.unflatten(df)

        expected = self.spark.createDataFrame(
            [(1, {"a": 1, "b": {"c": 2, "d": 3}}),
             (2, None),
             (3, {"a": 1, "b": {"c": 2, "d": 3}})],
            "id INT, s STRUCT<a:INT, b:STRUCT<c:INT, d:INT>>",
        )
        # fmt: on
        self.assertEqual(df2.sort("id").collect(), expected.sort("id").collect())

    def test_unflatten_with_complex_names(self):
        # fmt: off
        df = self.spark.createDataFrame(
            [(1, 1, 2, 3),
             (2, None, None, None),
             (3, 1, 2, 3)],
            "id INT, `s.s1?a.a1` INT, `s.s1?b.b1?c.c1` INT, `s.s1?b.b1?d.d1` INT",
        )
        # fmt: on

        df2 = spark_utils.unflatten(df, "?")

        expected = self.spark.createDataFrame(
            [
                (1, {"a.a1": 1, "b.b1": {"c.c1": 2, "d.d1": 3}}),
                (2, None),
                (3, {"a.a1": 1, "b.b1": {"c.c1": 2, "d.d1": 3}}),
            ],
            "id INT, `s.s1` STRUCT<`a.a1`:INT, `b.b1`:STRUCT<`c.c1`:INT, `d.d1`:INT>>",
        )
        self.assertEqual(df2.sort("id").collect(), expected.sort("id").collect())

    def test_unflatten_with_struct_and_col(self):
        """When we have a s.s1 struct and a `s.s2` column"""
        df = self.spark.createDataFrame([({"s1": 1}, 2)], "s STRUCT<s1: INT>, `s.s2` INT")

        df2 = spark_utils.unflatten(df)

        expected = self.spark.createDataFrame([({"s1": 1, "s2": 2},)], "s STRUCT<s1: INT, s2: INT>")

        self.assertEqual(df2.collect(), expected.collect())

    def test_unflatten_with_struct_and_col_2(self):
        """When we have a r.s.s1 struct and a r.`s.s2` column"""
        df = self.spark.createDataFrame([({"s": {"s1": 1}, "s.s2": 2},)], "r STRUCT<s: STRUCT<s1: INT>, `s.s2`: INT>")

        df2 = spark_utils.unflatten(df)

        expected = self.spark.createDataFrame([({"s": {"s1": 1, "s2": 2}},)], "r STRUCT<s: STRUCT<s1: INT, s2: INT>>")

        self.assertEqual(df2.collect(), expected.collect())

    def test_get_schema_from_json(self):
        df = self.spark.createDataFrame(
            [
                (1, '{"a": 1, "b": 1}', '{"c": 1, "d": 1}'),
                (2, '{"a": 2, "b": 2}', '{"c": 2, "d": 2}'),
                (3, '{"a": 3, "b": 3}', '{"c": 3, "d": 3}'),
            ],
            "id INT, json1 STRING, json2 STRING",
        )

        json_schema = df.schema.json()

        schema = spark_utils.get_schema_from_json(json_schema)

        self.assertEqual(
            schema,
            StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField("json1", StringType(), True),
                    StructField("json2", StringType(), True),
                ]
            ),
        )

    def test_df_schema_as_json_string(self):
        df = self.spark.createDataFrame(
            [
                (1, '{"a": 1, "b": 1}', '{"c": 1, "d": 1}'),
                (2, '{"a": 2, "b": 2}', '{"c": 2, "d": 2}'),
                (3, '{"a": 3, "b": 3}', '{"c": 3, "d": 3}'),
            ],
            "id INT, json1 STRING, json2 STRING",
        )

        schema = spark_utils.df_schema_as_json_string(df)

        self.assertEqual(
            schema,
            '{\n  "fields": [\n    {\n      "metadata": {},\n      "name": "id",\n'
            '      "nullable": true,\n      "type": "integer"\n    },\n    {\n'
            '      "metadata": {},\n      "name": "json1",\n      "nullable": true,\n'
            '      "type": "string"\n    },\n    {\n      "metadata": {},\n      "name": "json2",\n'
            '      "nullable": true,\n      "type": "string"\n    }\n  ],\n  "type": "struct"\n}',
        )

    def test_get_spark_session(self):
        session = get_spark_session()

        self.assertEqual(str(type(session)), "<class 'pyspark.sql.session.SparkSession'>")

    def test_structify(self):
        # fmt: off
        df = self.spark.createDataFrame(
            [(1, "a", "x"),
             (2, "b", "y"),
             (3, "c", "z")],
            "id INT, colA STRING, colB STRING"
        )

        expected = self.spark.createDataFrame(
            [(1, {"colA": "a", "colB": "x"}),
             (2, {"colA": "b", "colB": "y"}),
             (3, {"colA": "c", "colB": "z"})],
            "id INT, my_struct STRUCT<colA: STRING, colB: STRING>",
        )
        # fmt: on
        actual = spark_utils.structify(df, "my_struct", "id", deduplicate=False)

        self.assertEqual(actual.sort("id").collect(), expected.sort("id").collect())

    def test_structify_with_multiple_primary_keys(self):
        """structify should work with multiple primary keys"""
        # fmt: off
        df = self.spark.createDataFrame(
            [(1, 1, "a", "x"),
             (1, 2, "b", "y"),
             (2, 3, "c", "z")],
            "id1 INT, id2 INT, colA STRING, colB STRING"
        )
        # fmt: on

        expected = self.spark.createDataFrame(
            [
                (1, 1, {"colA": "a", "colB": "x"}),
                (1, 2, {"colA": "b", "colB": "y"}),
                (2, 3, {"colA": "c", "colB": "z"}),
            ],
            "id1 INT, id2 INT, my_struct STRUCT<colA: STRING, colB: STRING>",
        )
        actual = spark_utils.structify(df, "my_struct", ["id1", "id2"], deduplicate=False)

        self.assertEqual(actual.sort("id1", "id2").collect(), expected.sort("id1", "id2").collect())

    def test_structify_with_deduplicate(self):
        """structify should work with deduplication"""
        # fmt: off
        df = self.spark.createDataFrame(
            [(1, "a", "x"),
             (1, "b", "y"),
             (1, "c", "z")],
            "id INT, colA STRING, colB STRING"
        )
        # fmt: on

        expected = self.spark.createDataFrame(
            [(1, [{"colA": "a", "colB": "x"}, {"colA": "b", "colB": "y"}, {"colA": "c", "colB": "z"}])],
            "id INT, my_struct ARRAY<STRUCT<colA: STRING, colB: STRING>>",
        )
        actual = spark_utils.structify(df, "my_struct", "id", deduplicate=True)

        self.assertEqual(actual.sort("id").collect(), expected.sort("id").collect())

    def test_get_sortable_columns(self):
        """structify should work with deduplication"""
        df = self.spark.createDataFrame(
            [(1, "a", [1, 2], {"a": 1, "b": 2}, (1, 2))],
            "intCol INT, stringCol STRING, arrayCol ARRAY<INT>, "
            "mapCol MAP<STRING, INT>, structCol STRUCT<a: INT, b: INT>",
        )

        actual = spark_utils.get_sortable_columns(df)
        self.assertEqual(actual, ["`intCol`", "`stringCol`", "`arrayCol`", "`structCol`"])
        df.orderBy(*spark_utils.get_sortable_columns(df))

    def test_unpivot_with_complex_col_names(self):
        spark = self.spark
        df = spark.createDataFrame(
            [
                (2018, "Orange", None, 4000, None),
                (2018, "Beans", None, 1500, 2000),
                (2018, "Banana", 2000, 400, None),
                (2018, "Carrots", 2000, 1200, None),
                (2019, "Orange", 5000, None, 5000),
                (2019, "Beans", None, 1500, 2000),
                (2019, "Banana", None, 1400, 400),
                (2019, "Carrots", None, 200, None),
            ],
            "year INT, `product.type` STRING, `country.Canada` INT, `country.China` INT, `country.Mexico` INT",
        )

        actual = spark_utils.unpivot(df, ["year", "product.type"], key_alias="country.name", value_alias="total")
        expected = spark.createDataFrame(
            [
                (2018, "Banana", "country.Canada", 2000),
                (2018, "Banana", "country.China", 400),
                (2018, "Banana", "country.Mexico", None),
                (2018, "Beans", "country.Canada", None),
                (2018, "Beans", "country.China", 1500),
                (2018, "Beans", "country.Mexico", 2000),
                (2018, "Carrots", "country.Canada", 2000),
                (2018, "Carrots", "country.China", 1200),
                (2018, "Carrots", "country.Mexico", None),
                (2018, "Orange", "country.Canada", None),
                (2018, "Orange", "country.China", 4000),
                (2018, "Orange", "country.Mexico", None),
                (2019, "Banana", "country.Canada", None),
                (2019, "Banana", "country.China", 1400),
                (2019, "Banana", "country.Mexico", 400),
                (2019, "Beans", "country.Canada", None),
                (2019, "Beans", "country.China", 1500),
                (2019, "Beans", "country.Mexico", 2000),
                (2019, "Carrots", "country.Canada", None),
                (2019, "Carrots", "country.China", 200),
                (2019, "Carrots", "country.Mexico", None),
                (2019, "Orange", "country.Canada", 5000),
                (2019, "Orange", "country.China", None),
                (2019, "Orange", "country.Mexico", 5000),
            ],
            "year INT, `product.type` STRING, `country.name` STRING, total INT",
        )
        actual_sorted = actual.sort("year", "`product.type`", "`country.name`")
        expected_sorted = expected.sort("year", "`product.type`", "`country.name`")
        self.assertEqual(actual_sorted.collect(), expected_sorted.collect())

    def test_union_data_frames(self):
        left_df = self.spark.createDataFrame(
            [
                (1, {"a": 1, "b": 1}, {"c": 1, "d": 1}),
                (2, {"a": 2, "b": 2}, {"c": 2, "d": 2}),
                (3, {"a": 3, "b": 3}, {"c": 3, "d": 3}),
            ],
            "id INT, json1 STRUCT<a: BIGINT, b: BIGINT>, json2 STRUCT<c: BIGINT, d:BIGINT>",
        )

        right_df = self.spark.createDataFrame(
            [
                (4, {"a": 4, "b": 4}, {"c": 4, "d": 4, "e": 1}),
                (5, {"a": 5, "b": 5}, {"c": 5, "d": 5, "e": 2}),
                (6, {"a": 6, "b": 6}, {"c": 6, "d": 6, "e": 3}),
            ],
            "id INT, json1 STRUCT<a: BIGINT, b: BIGINT>, json3 STRUCT<c: BIGINT, d:BIGINT, e:BIGINT>",
        )

        expected_df = self.spark.createDataFrame(
            [
                (1, {"a": 1, "b": 1}, {"c": 1, "d": 1}, None),
                (2, {"a": 2, "b": 2}, {"c": 2, "d": 2}, None),
                (3, {"a": 3, "b": 3}, {"c": 3, "d": 3}, None),
                (4, {"a": 4, "b": 4}, None, {"c": 4, "d": 4, "e": 1}),
                (5, {"a": 5, "b": 5}, None, {"c": 5, "d": 5, "e": 2}),
                (6, {"a": 6, "b": 6}, None, {"c": 6, "d": 6, "e": 3}),
            ],
            """
            id INT,
            json1 STRUCT<a: BIGINT, b: BIGINT>,
            json2 STRUCT<c: BIGINT, d:BIGINT>,
            json3 STRUCT<c: BIGINT, d:BIGINT, e:BIGINT>
            """,
        )
        result = spark_utils.union_data_frames(left_df, right_df)
        self.assertEqual(
            result.sort("id", "json1", "json2", "json3").show(10),
            expected_df.sort("id", "json1", "json2", "json3").show(10),
        )
        result = spark_utils.union_data_frames(left_df, right_df, True)
        self.assertEqual(
            result.sort("id", "json1", "json2", "json3").show(10),
            expected_df.sort("id", "json1", "json2", "json3").show(10),
        )

    def test_get_col_type_with_wrong_column_name(self):
        df = self.spark.createDataFrame(
            [
                (1, {"a": "A"}),
                (2, {"a": "B"}),
                (3, {"a": "C"}),
            ],
            "id INT, s STRUCT<a:STRING>",
        )
        with self.assertRaises(ValueError) as cm:
            spark_utils.get_col_type(df.schema, "s.b")
        self.assertEqual('Cannot resolve column name "s.b"', str(cm.exception))

    def test_to_generic_typed_struct(self):
        df = self.spark.createDataFrame(
            [
                (1, {"name": "Paris"}, {"first_name": "Jacques", "last_name": "Dupont", "is_adult": True}),
                (2, {"name": "Paris"}, {"first_name": "Michel", "last_name": "Roger", "is_adult": False}),
                (3, {"name": "Paris"}, {"first_name": "Marie", "last_name": "De La Rue", "is_adult": True}),
            ],
            "id INT, location STRUCT<name:STRING>, "
            "person STRUCT<first_name:STRING, last_name:STRING, is_adult:BOOLEAN>",
        )
        expected_df = self.spark.createDataFrame(
            [
                Row(
                    id=1,
                    location=[
                        Row(
                            key="name",
                            type="string",
                            value=Row(
                                boolean=None,
                                bytes=None,
                                date=None,
                                float=None,
                                int=None,
                                string="Paris",
                                timestamp=None,
                            ),
                        )
                    ],
                    person=[
                        Row(
                            key="first_name",
                            type="string",
                            value=Row(
                                boolean=None,
                                bytes=None,
                                date=None,
                                float=None,
                                int=None,
                                string="Jacques",
                                timestamp=None,
                            ),
                        ),
                        Row(
                            key="last_name",
                            type="string",
                            value=Row(
                                boolean=None,
                                bytes=None,
                                date=None,
                                float=None,
                                int=None,
                                string="Dupont",
                                timestamp=None,
                            ),
                        ),
                        Row(
                            key="is_adult",
                            type="boolean",
                            value=Row(
                                boolean=True, bytes=None, date=None, float=None, int=None, string=None, timestamp=None
                            ),
                        ),
                    ],
                ),
                Row(
                    id=2,
                    location=[
                        Row(
                            key="name",
                            type="string",
                            value=Row(
                                boolean=None,
                                bytes=None,
                                date=None,
                                float=None,
                                int=None,
                                string="Paris",
                                timestamp=None,
                            ),
                        )
                    ],
                    person=[
                        Row(
                            key="first_name",
                            type="string",
                            value=Row(
                                boolean=None,
                                bytes=None,
                                date=None,
                                float=None,
                                int=None,
                                string="Michel",
                                timestamp=None,
                            ),
                        ),
                        Row(
                            key="last_name",
                            type="string",
                            value=Row(
                                boolean=None,
                                bytes=None,
                                date=None,
                                float=None,
                                int=None,
                                string="Roger",
                                timestamp=None,
                            ),
                        ),
                        Row(
                            key="is_adult",
                            type="boolean",
                            value=Row(
                                boolean=False, bytes=None, date=None, float=None, int=None, string=None, timestamp=None
                            ),
                        ),
                    ],
                ),
                Row(
                    id=3,
                    location=[
                        Row(
                            key="name",
                            type="string",
                            value=Row(
                                boolean=None,
                                bytes=None,
                                date=None,
                                float=None,
                                int=None,
                                string="Paris",
                                timestamp=None,
                            ),
                        )
                    ],
                    person=[
                        Row(
                            key="first_name",
                            type="string",
                            value=Row(
                                boolean=None,
                                bytes=None,
                                date=None,
                                float=None,
                                int=None,
                                string="Marie",
                                timestamp=None,
                            ),
                        ),
                        Row(
                            key="last_name",
                            type="string",
                            value=Row(
                                boolean=None,
                                bytes=None,
                                date=None,
                                float=None,
                                int=None,
                                string="De La Rue",
                                timestamp=None,
                            ),
                        ),
                        Row(
                            key="is_adult",
                            type="boolean",
                            value=Row(
                                boolean=True, bytes=None, date=None, float=None, int=None, string=None, timestamp=None
                            ),
                        ),
                    ],
                ),
            ],
            f"""id INT, location {generic_typed_struct_type}, person {generic_typed_struct_type}""",
        )
        result = spark_utils.to_generic_typed_struct(df, ["location", "person"])
        self.assertEqual(result.sort("id").collect(), expected_df.sort("id").collect())

    def test_to_generic_typed_struct_with_decimal_types(self):
        df = self.spark.createDataFrame(
            [
                (1, {"a": "A", "decimal_col": Decimal(1.1)}),
                (2, {"a": "B", "decimal_col": Decimal(2.2)}),
                (3, {"a": "C", "decimal_col": Decimal(3.3)}),
            ],
            "id INT, s STRUCT<a:STRING, decimal_col:DECIMAL(10, 2)>",
        )
        expected_df = self.spark.createDataFrame(
            [
                Row(
                    id=1,
                    s=[
                        Row(
                            key="a",
                            type="string",
                            value=Row(
                                date=None, timestamp=None, int=None, float=None, boolean=None, string="A", bytes=None
                            ),
                        ),
                        Row(
                            key="decimal_col",
                            type="float",
                            value=Row(
                                date=None, timestamp=None, int=None, float=1.1, boolean=None, string=None, bytes=None
                            ),
                        ),
                    ],
                ),
                Row(
                    id=2,
                    s=[
                        Row(
                            key="a",
                            type="string",
                            value=Row(
                                date=None, timestamp=None, int=None, float=None, boolean=None, string="B", bytes=None
                            ),
                        ),
                        Row(
                            key="decimal_col",
                            type="float",
                            value=Row(
                                date=None, timestamp=None, int=None, float=2.2, boolean=None, string=None, bytes=None
                            ),
                        ),
                    ],
                ),
                Row(
                    id=3,
                    s=[
                        Row(
                            key="a",
                            type="string",
                            value=Row(
                                date=None, timestamp=None, int=None, float=None, boolean=None, string="C", bytes=None
                            ),
                        ),
                        Row(
                            key="decimal_col",
                            type="float",
                            value=Row(
                                date=None, timestamp=None, int=None, float=3.3, boolean=None, string=None, bytes=None
                            ),
                        ),
                    ],
                ),
            ],
            f"""id INT, s {generic_typed_struct_type}""",
        ).withColumnRenamed("person", "person.struct")
        result = spark_utils.to_generic_typed_struct(df, ["s"])
        self.assertEqual(result.sort("id").collect(), expected_df.sort("id").collect())

    def test_to_generic_typed_struct_with_unsupported_types(self):
        df = self.spark.createDataFrame(
            [
                (1, {"a": "A", "array_col": [1, 2, 3]}),
                (2, {"a": "B", "array_col": [1, 2, 3]}),
                (3, {"a": "C", "array_col": [1, 2, 3]}),
            ],
            "id INT, s STRUCT<a:STRING, array_col:ARRAY<INT>>",
        )
        expected_df = self.spark.createDataFrame(
            [
                Row(
                    id=1,
                    s=[
                        Row(
                            key="a",
                            type="string",
                            value=Row(
                                date=None, timestamp=None, int=None, float=None, boolean=None, string="A", bytes=None
                            ),
                        ),
                    ],
                ),
                Row(
                    id=2,
                    s=[
                        Row(
                            key="a",
                            type="string",
                            value=Row(
                                date=None, timestamp=None, int=None, float=None, boolean=None, string="B", bytes=None
                            ),
                        ),
                    ],
                ),
                Row(
                    id=3,
                    s=[
                        Row(
                            key="a",
                            type="string",
                            value=Row(
                                date=None, timestamp=None, int=None, float=None, boolean=None, string="C", bytes=None
                            ),
                        ),
                    ],
                ),
            ],
            f"""id INT, s {generic_typed_struct_type}""",
        ).withColumnRenamed("person", "person.struct")
        result = spark_utils.to_generic_typed_struct(df, ["s"])
        self.assertEqual(result.sort("id").collect(), expected_df.sort("id").collect())

    def test_to_generic_typed_struct_with_weird_column_names(self):
        df = self.spark.createDataFrame(
            [
                (1, {"c.`.d": "A"}),
                (2, {"c.`.d": "B"}),
                (3, {"c.`.d": "C"}),
            ],
            "id INT, `a.``.b` STRUCT<`c.``.d`:STRING>",
        )
        expected_df = self.spark.createDataFrame(
            [
                Row(
                    id=1,
                    s=[
                        Row(
                            key="c.`.d",
                            type="string",
                            value=Row(
                                date=None, timestamp=None, int=None, float=None, boolean=None, string="A", bytes=None
                            ),
                        ),
                    ],
                ),
                Row(
                    id=2,
                    s=[
                        Row(
                            key="c.`.d",
                            type="string",
                            value=Row(
                                date=None, timestamp=None, int=None, float=None, boolean=None, string="B", bytes=None
                            ),
                        ),
                    ],
                ),
                Row(
                    id=3,
                    s=[
                        Row(
                            key="c.`.d",
                            type="string",
                            value=Row(
                                date=None, timestamp=None, int=None, float=None, boolean=None, string="C", bytes=None
                            ),
                        ),
                    ],
                ),
            ],
            f"""id INT, s {generic_typed_struct_type}""",
        ).withColumnRenamed("s", "a.`.b")
        result = spark_utils.to_generic_typed_struct(df, ["`a.``.b`"])
        self.assertEqual(result.sort("id").collect(), expected_df.sort("id").collect())

    def test_camel_case_fields_to_snake_case(self):
        df = self.spark.createDataFrame(
            [(1, "a", [1, 2], {"a": 1, "b": 2}, (1, [1, 2], {"a": 1, "b": 2}))],
            "intCol INT, StringCol STRING, arrayCol ARRAY<INT>, MapCol MAP<STRING, INT>, "
            "structCol STRUCT<a: INT, InnerArrayCol: ARRAY<INT>, InnerMapCol: MAP<STRING, INT>>",
        )
        expected_df = self.spark.createDataFrame(
            [(1, "a", [1, 2], {"a": 1, "b": 2}, (1, [1, 2], {"a": 1, "b": 2}))],
            "int_col INT, string_col STRING, array_col ARRAY<INT>, map_col MAP<STRING, INT>, "
            "struct_col STRUCT<a: INT, inner_array_col: ARRAY<INT>, inner_map_col: MAP<STRING, INT>>",
        )

        result = spark_utils.camel_case_fields_to_snake_case(df)
        self.assertEqual(result.collect(), expected_df.collect())

    def test_convert_column_type(self):
        df = self.spark.createDataFrame([Row(i=1, s="str", b=True, d=3.8)], "i INT, s STRING, b BOOLEAN, d DOUBLE")
        result = spark_utils.convert_column_type(df, input_type="boolean", converted_type="int")
        expected_df = self.spark.createDataFrame([Row(i=1, s="str", b=1, d=3.8)], "i INT, s STRING, b INT, d DOUBLE")
        self.assertEqual(result.collect(), expected_df.collect())

        result = spark_utils.convert_column_type(df, input_type="int", converted_type="string")
        expected_df = self.spark.createDataFrame(
            [Row(i="1", s="str", b=True, d=3.8)], "i STRING, s STRING, b BOOLEAN, d DOUBLE"
        )
        self.assertEqual(result.collect(), expected_df.collect())
