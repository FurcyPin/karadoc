from unittest import TestCase

from karadoc.common.exceptions import ActionFileLoadingError
from karadoc.common.job_core.load import __load_file as load_file
from karadoc.common.job_core.load import load_non_runnable_action_file
from karadoc.spark.batch.spark_batch_job import SparkBatchJob
from karadoc.spark.quality.quality_check_job import QualityCheckJob
from karadoc.spark.stream.spark_stream_job import SparkStreamJob
from tests.karadoc.test_utils import get_resource_folder_path

model_dir = get_resource_folder_path(__name__) + "/model"


class TestLoad(TestCase):
    def test_load_file(self):
        job = load_file(
            "test_schema",
            "test_table",
            f"{model_dir}/simple/test_schema.db/test_table/POPULATE.py",
            "POPULATE",
            {},
            SparkBatchJob,
        )
        job.init()
        df = job.run()
        self.assertEqual(df.count(), 1)
        self.assertEqual(df.collect()[0]["value"], "this is a test DataFrame")

    def test_load_file_with_wrong_type(self):
        with self.assertRaises(ActionFileLoadingError) as context:
            load_file(
                "test_schema",
                "test_table",
                f"{model_dir}/simple/test_schema.db/test_table/POPULATE.py",
                "POPULATE",
                {},
                SparkStreamJob,
            )
        e = context.exception
        self.assertIn("not of the expected type", e.args[0])

    def test_load_action_files_with_same_name(self):
        """
        Given two action files from different models
        When loading the two files one after the other
        Then the second file should not have remains from the first
        (this is a bug that happened during unit test suites)
        """
        load_file(
            "test_schema",
            "test_table",
            f"{model_dir}/double_1/test_schema.db/test_table/QUALITY_CHECK.py",
            "POPULATE",
            {},
            QualityCheckJob,
        )
        job2 = load_file(
            "test_schema",
            "test_table",
            f"{model_dir}/double_2/test_schema.db/test_table/QUALITY_CHECK.py",
            "POPULATE",
            {},
            QualityCheckJob,
        )
        self.assertEqual(1, len(job2.alerts))

    def test_load_non_runnable_action_file_with_file_not_found(self):
        """
        GIVEN a model
        WHEN loading a file that does not exist
        THEN an intelligible error message should be raised
        """
        with self.assertRaises(FileNotFoundError) as e:
            load_non_runnable_action_file(
                "test_schema.table_does_not_exist",
                SparkBatchJob,
            )
        self.assertIn(
            "Found no file matching the path '**/test_schema.db/table_does_not_exist/POPULATE.py' "
            "for the table test_schema.table_does_not_exist in the directory 'model'",
            str(e.exception),
        )

    def test_load_runnable_action_file_with_file_not_found(self):
        """
        GIVEN a model
        WHEN loading a file that does not exist
        THEN an intelligible error message should be raised
        """
        with self.assertRaises(FileNotFoundError) as e:
            load_non_runnable_action_file(
                "test_schema.table_does_not_exist",
                SparkBatchJob,
            )
        self.assertIn(
            "Found no file matching the path '**/test_schema.db/table_does_not_exist/POPULATE.py' "
            "for the table test_schema.table_does_not_exist in the directory 'model'",
            str(e.exception),
        )
