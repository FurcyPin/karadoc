from karadoc.spark.batch.spark_batch_job import SparkBatchJob
from karadoc.test_utils.mock_settings import mock_settings_for_test_class
from karadoc.test_utils.pyspark_test_class import PySparkTest
from karadoc.test_utils.spark import MockDataFrame, MockRow


@mock_settings_for_test_class(
    {
        "connection": {
            "test": {
                "type": "connector_mock",
                "mock": "tests.resources.karadoc.spark.connectors.test_connector_mock.connector_mock",
            }
        }
    }
)
class TestConnectorMock(PySparkTest):
    def test_failed_authentication(self):
        source = {"connection": "test"}
        job = SparkBatchJob()
        job.init()
        mock_conn = job.get_input_connector(source)
        df = mock_conn.read(source)
        self.assertEqual(MockDataFrame([MockRow(a="a")]), df)
