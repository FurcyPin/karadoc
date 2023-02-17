import adal
from pyspark.sql import DataFrame, SparkSession

from karadoc.common.conf import ConfBox
from karadoc.spark.spark_connector import SparkConnector

JDBC_SPARK = "com.microsoft.sqlserver.jdbc.spark"


class MssqlConnector(SparkConnector):
    def __init__(self, spark: SparkSession, connection_conf: ConfBox):
        super().__init__(spark, connection_conf)

    def _get_url(self):
        return "jdbc:sqlserver://%s;databaseName=%s;" % (self.conf.get("host"), self.conf.get("database"))

    def _get_access_token(self):
        client_id = self.conf.get("client_id")
        client_secret = self.conf.get("client_secret")
        tenant_id = self.conf.get("tenant_id")
        authority = "https://login.microsoftonline.com/%s" % tenant_id
        context = adal.AuthenticationContext(authority)
        token = context.acquire_token_with_client_credentials("https://database.windows.net", client_id, client_secret)
        return token["accessToken"]

    def _get_read_options(self, source: dict):
        url = self._get_url()
        access_token = self._get_access_token()
        query = source.get("query")
        table_name = source.get("table")

        read_options = {
            "url": url,
            "accessToken": access_token,
            "encrypt": "true",
            "hostNameInCertificate": "*.database.windows.net",
            "isolationLevel": "READ_UNCOMMITTED",
        }

        if query is not None:
            read_options["query"] = query
        else:
            read_options["dbtable"] = table_name

        read_options.update(source.get("options", {}))
        return read_options

    def _get_write_options(self, dest: dict):
        url = self._get_url()
        access_token = self._get_access_token()
        table_name = dest.get("table")

        write_options = {
            "url": url,
            "dbtable": table_name,
            "accessToken": access_token,
            "encrypt": "true",
            "hostNameInCertificate": "*.database.windows.net",
        }
        write_options.update(dest.get("options", {}))
        return write_options

    def read(self, source: dict):
        """Read data from an external input via a spark connector for SQL Server.

        If the input table has columns that contains dots ('.'), these are
        automatically replaced by underscores to avoid bugs downstream.

        The source must have the following attributes:

        - "connection": name of the jdbc connection
        - "table": name of the table
        - "query": an optional custom query that can be used instead of "table".
        - "options": (Optional[Dict[str, str]]) extra options to pass to the spark connector read method
        # https://github.com/microsoft/sql-spark-connector#supported-options

        The corresponding mssql connection must have the following attributes:

        - type = 'mssql'
        - host = Server name
        - database = Database name
        - client_id = 'client-id'
        - client_secret = 'client-secret'
        - tenant_id = 'tenant-id'

        :param source: a dictionary describing the source
        :return: a DataFrame
        """
        options = self._get_read_options(source)
        df = self.spark.read.format(JDBC_SPARK).options(**options).load()
        # Replace dots with underscore in column names
        df = df.toDF(*[c.replace(".", "_") for c in df.columns])
        return df

    def write(self, df: DataFrame, dest) -> None:
        """Write dataframe to an external output via a jdbc connection.

        The dest must have the following attributes:
        - "connection": name of the jdbc connection
        - "table": name of the table
        - "options": (optional) extra options to pass to the spark connector write method
        # https://github.com/microsoft/sql-spark-connector#supported-options

        The corresponding jdbc connection must have the following attributes:

        - type = 'mssql'
        - host = Server name
        - database = Database name
        - client_id = 'client-id'
        - client_secret = 'client-secret'
        - tenant_id = 'tenant-id'

        :param df: the DataFrame to export
        :param dest: A dictionary describing the destination
        :return:
        """
        mode = dest["mode"]
        options = self._get_write_options(dest)
        df.write.format(JDBC_SPARK).mode(mode).options(**options).save()

    def test_connection(self):
        options = self._get_read_options({"query": "SELECT 1 as ok"})
        df = self.spark.read.format(JDBC_SPARK).options(**options).load()
        if df.collect()[0].ok == 1:
            return True
        else:
            df.show()
            return False
