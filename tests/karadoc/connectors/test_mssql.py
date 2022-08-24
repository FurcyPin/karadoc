from unittest import mock

from karadoc.common.connector import load_connector
from karadoc.connectors.mssql import MssqlConnector
from karadoc.test_utils import pyspark_test_class
from karadoc.test_utils.mock_settings import mock_settings_for_test_class
from karadoc.test_utils.spark import MockDataFrame, MockRow


def token(*args):
    return {"accessToken": "test_token"}


@mock_settings_for_test_class(
    {
        "connection": {
            "sql-test": {
                "type": "karadoc.connectors.mssql",
                "host": "test_host",
                "database": "test_db",
            }
        }
    }
)
@mock.patch("adal.AuthenticationContext.acquire_token_with_client_credentials", token)
class TestMssqlConnector(pyspark_test_class.PySparkTest):
    def test_get_url(self):
        mssql = load_connector("sql-test", None)  # type: MssqlConnector
        self.assertEqual(mssql._get_url(), "jdbc:sqlserver://test_host;databaseName=test_db;")

    def test_get_read_options(self):
        mssql = load_connector("sql-test", None)  # type: MssqlConnector
        source = {"connection": "sql-test", "table": "schema.table"}
        actual_options = mssql._get_read_options(source)
        expected_options = {
            "dbtable": "schema.table",
            "url": "jdbc:sqlserver://test_host;databaseName=test_db;",
            "accessToken": "test_token",
            "encrypt": "true",
            "hostNameInCertificate": "*.database.windows.net",
            "isolationLevel": "READ_UNCOMMITTED",
        }
        self.assertDictEqual(expected_options, actual_options)

    def test_get_read_options_with_query(self):
        mssql = load_connector("sql-test", None)  # type: MssqlConnector
        source = {"connection": "sql-test", "query": "SELECT * FROM schema.table"}
        actual_options = mssql._get_read_options(source)
        expected_options = {
            "query": "SELECT * FROM schema.table",
            "url": "jdbc:sqlserver://test_host;databaseName=test_db;",
            "accessToken": "test_token",
            "encrypt": "true",
            "hostNameInCertificate": "*.database.windows.net",
            "isolationLevel": "READ_UNCOMMITTED",
        }
        self.assertDictEqual(expected_options, actual_options)

    def test_get_read_options_with_query_and_table(self):
        mssql = load_connector("sql-test", None)  # type: MssqlConnector
        source = {
            "connection": "sql-test",
            "query": "SELECT * FROM schema.table",
            "table": "schema.table",
        }
        actual_options = mssql._get_read_options(source)
        expected_options = {
            "query": "SELECT * FROM schema.table",
            "url": "jdbc:sqlserver://test_host;databaseName=test_db;",
            "accessToken": "test_token",
            "encrypt": "true",
            "hostNameInCertificate": "*.database.windows.net",
            "isolationLevel": "READ_UNCOMMITTED",
        }
        self.assertDictEqual(expected_options, actual_options)

    def test_get_read_options_with_option_override(self):
        mssql = load_connector("sql-test", None)  # type: MssqlConnector
        source = {"connection": "sql-test", "table": "schema.table", "options": {"isolationLevel": "READ_COMMITTED"}}
        actual_options = mssql._get_read_options(source)
        expected_options = {
            "dbtable": "schema.table",
            "url": "jdbc:sqlserver://test_host;databaseName=test_db;",
            "accessToken": "test_token",
            "encrypt": "true",
            "hostNameInCertificate": "*.database.windows.net",
            "isolationLevel": "READ_COMMITTED",
        }
        self.assertDictEqual(expected_options, actual_options)

    def test_read(self):
        mssql = load_connector("sql-test", self.spark)  # type: MssqlConnector
        source = {"connection": "sql-test", "table": "schema.table"}

        def load(*args, **kwargs):
            return self.spark.sql("SELECT 1 as a")

        with mock.patch("pyspark.sql.DataFrameReader.load", side_effect=load, autospec=True) as mock_load:
            df = mssql.read(source)

        mock_load.assert_called_once()
        self.assertEqual(MockDataFrame([MockRow(a=1)]), df)

    def test_read_with_query(self):
        mssql = load_connector("sql-test", self.spark)  # type: MssqlConnector
        source = {"connection": "sql-test", "query": "SELECT * FROM schema.table"}

        def load(*args, **kwargs):
            return self.spark.sql("SELECT 1 as a")

        with mock.patch("pyspark.sql.DataFrameReader.load", side_effect=load, autospec=True) as mock_load:
            df = mssql.read(source)

        mock_load.assert_called_once()
        self.assertEqual(MockDataFrame([MockRow(a=1)]), df)

    def test_read_with_rename(self):
        mssql = load_connector("sql-test", self.spark)  # type: MssqlConnector
        source = {"connection": "sql-test", "table": "schema.table"}

        def load(*args, **kwargs):
            return self.spark.sql("SELECT 1 as `a.b`")

        with mock.patch("pyspark.sql.DataFrameReader.load", side_effect=load, autospec=True) as mock_load:
            df = mssql.read(source)

        mock_load.assert_called_once()
        self.assertEqual(MockDataFrame([MockRow(a_b=1)]), df)

    def test_get_write_options(self):
        mssql = load_connector("sql-test", None)  # type: MssqlConnector
        source = {"connection": "sql-test", "table": "schema.table"}

        actual_options = mssql._get_write_options(source)
        expected_options = {
            "dbtable": "schema.table",
            "url": "jdbc:sqlserver://test_host;databaseName=test_db;",
            "accessToken": "test_token",
            "encrypt": "true",
            "hostNameInCertificate": "*.database.windows.net",
        }
        self.assertDictEqual(expected_options, actual_options)

    def test_write(self):
        mssql = load_connector("sql-test", self.spark)  # type: MssqlConnector
        dest = {
            "connection": "sql-test",
            "table": "schema.table",
            "mode": "overwrite",
        }

        with mock.patch("pyspark.sql.DataFrameWriter.save") as mock_save:
            df = self.spark.sql("SELECT 1 as a")
            mssql.write(df, dest)

        mock_save.assert_called_once()
