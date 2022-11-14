from unittest import TestCase

from karadoc.spark.batch.exec import load_populate, load_runnable_populate
from karadoc.test_utils.mock_settings import mock_settings_for_test_class


@mock_settings_for_test_class(
    {"enable_file_index_cache": False, "model_dir": "tests/resources/karadoc/spark/batch/test_batch_exec/model"}
)
class TestRunExec(TestCase):
    def test_load_runnable_populate(self):
        job = load_runnable_populate("test_schema.test_table", {})
        job.init()
        df = job.run()
        self.assertEqual(df.count(), 1)
        self.assertEqual(df.collect()[0]["value"], "this is a test DataFrame")

    def test_load_populate(self):
        job = load_populate("test_schema.test_table")
        job.init()
        with self.assertRaises(Exception) as cm:
            job.run()
        self.assertIn("use load_runnable_action_file instead", str(cm.exception))

    def test_load_runnable_populate_with_relative_imports(self):
        job = load_runnable_populate("test_schema.relative_import", {})
        job.init()
        df = job.run()
        self.assertEqual(df.count(), 1)
        self.assertEqual(df.collect()[0]["value"], "this is a test DataFrame with relative imports")
