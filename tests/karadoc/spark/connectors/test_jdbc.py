import unittest

from karadoc.spark.connectors.jdbc import JdbcConnector
from karadoc.spark.spark_connector import load_connector
from karadoc.test_utils.mock_settings import mock_settings_for_test_class


@mock_settings_for_test_class(
    {
        "connection": {
            "sql-test": {
                "type": "karadoc.spark.connectors.jdbc",
                "protocol": "sqlserver",
                "host": "test_host",
                "user": "test_user",
                "password": "test_password",
                "database": "test_db",
            }
        }
    }
)
class Jdbc(unittest.TestCase):
    def test_get_url(self):
        jdbc = load_connector("sql-test", None)  # type: JdbcConnector
        self.assertEqual(
            jdbc._get_url(),
            "jdbc:sqlserver://test_host;user=test_user;password=test_password;database=test_db;applicationName=karadoc",
        )
