import shutil
from unittest import TestCase

from karadoc.common import conf
from karadoc.common.stream import load_runnable_stream_file, load_stream_file
from karadoc.common.stream_utils import stream_to_batch
from karadoc.test_utils.mock_settings import mock_settings_for_test_class
from karadoc.test_utils.spark import MockDataFrame, MockRow

# For some reason, when applied on the class, a patch decorator will NOT be applied to the setUp and tearDown functions
# For this reason, we define it here once, then apply it to the class AND the setUp and tearDown methods
config_mock = mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": "tests/resources/karadoc/common/stream/model",
        "spark_stream_dir": "test_working_dir/spark/stream",
    }
)


@config_mock
class TestStreamExec(TestCase):
    @config_mock
    def setUp(self) -> None:
        shutil.rmtree(conf.get_spark_stream_tmp_dir(), ignore_errors=True)

    @config_mock
    def tearDown(self) -> None:
        shutil.rmtree(conf.get_spark_stream_tmp_dir(), ignore_errors=True)

    def test_load_runnable_stream_file(self):
        job = load_runnable_stream_file("test_schema.test_table", {})
        job.init()
        df = job.stream()
        self.assertEqual(MockDataFrame([MockRow(value="this is a test DataFrame")]), stream_to_batch(df))

    def test_load_stream_file(self):
        job = load_stream_file("test_schema.test_table")
        job.init()
        with self.assertRaises(Exception) as cm:
            job.stream()
        self.assertIn("use load_runnable_action_file instead", str(cm.exception))
