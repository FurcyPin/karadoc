from pyspark import StorageLevel
from spark_frame.schema_utils import schema_from_json

from karadoc.spark.analyze import timeline_analysis
from karadoc.test_utils import pyspark_test_class
from tests.karadoc.test_utils import get_resource_folder_path

test_resource_dir = get_resource_folder_path(__name__)


class TestTimelineAnalysis(pyspark_test_class.PySparkTest):
    source_df_json_schema = """
    {
      "fields": [
        {
          "metadata": {},
          "name": "id",
          "nullable": true,
          "type": "integer"
        },
        {
          "metadata": {},
          "name": "cohort",
          "nullable": true,
          "type": "string"
        },
        {
          "metadata": {},
          "name": "reference_date",
          "nullable": true,
          "type": "string"
        },
        {
          "metadata": {},
          "name": "float_col_1",
          "nullable": true,
          "type": "double"
        },
        {
          "metadata": {},
          "name": "float_col_2",
          "nullable": true,
          "type": "double"
        },
        {
          "metadata": {},
          "name": "int_col",
          "nullable": true,
          "type": "integer"
        },
        {
          "metadata": {},
          "name": "date_col_1",
          "nullable": true,
          "type": "date"
        },
        {
          "metadata": {},
          "name": "date_col_2",
          "nullable": true,
          "type": "date"
        },
        {
          "metadata": {},
          "name": "string_col",
          "nullable": true,
          "type": "string"
        },
        {
          "metadata": {},
          "name": "always_null_col",
          "nullable": true,
          "type": "string"
        }
      ],
      "type": "struct"
    }
    """

    def test_get_always_null_columns(self):
        df = self.spark.read.option("header", "true").csv(test_resource_dir + "/sample_file")
        actual = timeline_analysis._get_always_null_columns(df)
        expected = ["always_null_col"]
        self.assertEqual(expected, actual)

    def test_analyse_timeline(self):
        source_schema = schema_from_json(self.source_df_json_schema)
        df = self.spark.read.option("header", "true").schema(source_schema).csv(test_resource_dir + "/sample_file")
        actual = timeline_analysis.analyse_timeline(
            df, reference_time_col="reference_date", cohort_cols=["cohort"], nb_buckets=5
        ).persist(StorageLevel.DISK_ONLY)

        print(actual.schema.simpleString())
        expected_schema = (
            "struct<cohort:string,key:string,val:string,is_other:boolean,val_count:bigint,val_frequency:double>"
        )
        self.assertEqual(actual.schema.simpleString(), expected_schema)

        actual.createOrReplaceTempView("actual")

        check_df_1 = self.spark.sql(
            """
            SELECT
                key,
                SUM(val_count) as val_count
            FROM actual
            GROUP BY key
        """
        )

        expected_df_1 = self.spark.createDataFrame(
            [
                ("always_null_col", 6398),
                ("date_col_1", 6398),
                ("date_col_2", 6398),
                ("float_col_1", 6398),
                ("float_col_2", 6398),
                ("id", 6398),
                ("int_col", 6398),
                ("string_col", 6398),
            ],
            "key STRING, val_count INT",
        )
        self.assertEqual(check_df_1.sort("key").collect(), expected_df_1.sort("key").collect())

        check_df_2 = self.spark.sql(
            """
            SELECT
                key,
                COUNT(DISTINCT val) as nb_buckets
            FROM actual
            GROUP BY key
        """
        )

        expected_df_2 = self.spark.createDataFrame(
            [
                ("always_null_col", 0),
                ("date_col_1", 5),
                ("date_col_2", 5),
                ("float_col_1", 5),
                ("float_col_2", 1),
                ("id", 5),
                ("int_col", 5),
                ("string_col", 4),
            ],
            "key STRING, nb_buckets INT",
        )
        self.assertEqual(check_df_2.sort("key").collect(), expected_df_2.sort("key").collect())

        check_df_3 = self.spark.sql(
            """
            SELECT
                cohort,
                key,
                ROUND(SUM(val_frequency), 3) as total_frequency
            FROM actual
            GROUP BY cohort, key
            HAVING total_frequency <> 1.0
        """
        )
        self.assertTrue(check_df_3.count() == 0)

    def test_analyse_timeline_with_multiple_cohort_columns(self):
        source_schema = schema_from_json(self.source_df_json_schema)
        df = self.spark.read.option("header", "true").schema(source_schema).csv(test_resource_dir + "/sample_file")
        actual = timeline_analysis.analyse_timeline(
            df, reference_time_col="reference_date", cohort_cols=["cohort", "string_col"], nb_buckets=5
        ).persist(StorageLevel.DISK_ONLY)

        expected_schema = (
            "struct<cohort:string,string_col:string,key:string,"
            "val:string,is_other:boolean,val_count:bigint,val_frequency:double>"
        )
        self.assertEqual(actual.schema.simpleString(), expected_schema)

        actual.createOrReplaceTempView("actual")

        check_df_1 = self.spark.sql(
            """
            SELECT
                key,
                SUM(val_count) as val_count
            FROM actual
            GROUP BY key
        """
        )
        check_df_1.show()

        expected_df_1 = self.spark.createDataFrame(
            [
                ("always_null_col", 6398),
                ("date_col_1", 6398),
                ("date_col_2", 6398),
                ("float_col_1", 6398),
                ("float_col_2", 6398),
                ("id", 6398),
                ("int_col", 6398),
            ],
            "key STRING, val_count INT",
        )
        self.assertEqual(check_df_1.sort("key").collect(), expected_df_1.sort("key").collect())

        check_df_2 = self.spark.sql(
            """
            SELECT
                key,
                COUNT(DISTINCT val) as nb_buckets
            FROM actual
            GROUP BY key
        """
        )
        check_df_2.show()

        expected_df_2 = self.spark.createDataFrame(
            [
                ("always_null_col", 0),
                ("date_col_1", 5),
                ("date_col_2", 5),
                ("float_col_1", 5),
                ("float_col_2", 1),
                ("id", 5),
                ("int_col", 5),
            ],
            "key STRING, nb_buckets INT",
        )
        self.assertEqual(check_df_2.sort("key").collect(), expected_df_2.sort("key").collect())
