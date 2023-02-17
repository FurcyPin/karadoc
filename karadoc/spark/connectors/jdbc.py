from pyspark.sql import DataFrame, SparkSession

from karadoc.common.conf import APPLICATION_NAME, ConfBox
from karadoc.common.conf.configurable_class import ConfParam
from karadoc.spark.spark_connector import ConfigurableSparkConnector


class JdbcConnector(ConfigurableSparkConnector):
    """This connector leverages `Spark's jdbc <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html>`_
    connector.

    It builds the following JDBC url from its configuration parameters:
    "jdbc:{protocol}://{host};user={user};password={password};database={database}"

    It requires to have the jars of the corresponding jdbc protocol available in the path of the Spark JVMs.
    """

    protocol = ConfParam(
        required=True,
        type=str,
        description="JDBC protocol to use (e.g. sqlserver, psql)."
        "A jar with the corresponding protocol implementation must be available in Spark's JVM path.",
    )
    host = ConfParam(
        required=True, type=str, description="Hostname of the SQL server (e.g. example.database.windows.net)."
    )
    user = ConfParam(required=True, type=str, description="Name of the SQL user.")
    password = ConfParam(
        required=True, type=str, secret=True, description="Password (should be stored in a secret vault)."
    )
    database = ConfParam(required=True, type=str, description="SQL database to connect to.")

    def __init__(self, spark: SparkSession, connection_conf: ConfBox):
        super().__init__(spark, connection_conf)

    def _get_url(self):
        protocol = self.protocol.get()
        host = self.host.get()
        user = self.user.get()
        password = self.password.get()
        database = self.database.get()
        url = (
            f"jdbc:{protocol}://{host}"
            f";user={user};password={password};database={database};applicationName={APPLICATION_NAME}"
        )
        return url

    def read(self, source: dict):
        """Read data from an external input via
        `Spark's jdbc <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html>`_ connection.

        If the input table has column that contains dots ('.'), these are
        automatically replaced by underscores to avoid bugs downstream.

        The source must have the following attributes:

        - "connection": name of the jdbc connection
        - "table": name of the table
        - "custom_query": an optional custom query that can be use instead of "table" for advanced use.
        The query must be written as a sub-select (ex: "(SELECT colA from table) as T")

        The corresponding jdbc connection must have the following attributes:

        - type = 'jdbc'
        - protocol = 'sqlserver'
        - host = Server name
        - user = User name
        - password = Password
        - database = Database name

        Known limitations:

        - When using the "table" attribute automatically adds a "WITH (NOLOCK)" hint, which is not compatible
        with Azure Datawarehouse's external tables.

        :param source: a dictionary describing the source
        :return: a DataFrame
        """
        sql_url = self._get_url() + ";ApplicationIntent=ReadOnly"
        custom_query = source.get("custom_query")
        if custom_query is not None:
            df = self.spark.read.jdbc(sql_url, custom_query)
        else:
            df = (
                self.spark.read.format("jdbc")
                .option("url", sql_url)
                .option("dbtable", source["table"])
                .option("isolationLevel", "READ_UNCOMMITTED")
                .load()
            )
        # Replace dots with underscore in column names
        df = df.toDF(*[c.replace(".", "_") for c in df.columns])
        return df

    def write(self, df: DataFrame, dest) -> None:
        """Write dataframe to an external output via
        `Spark's jdbc <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html>`_ connection.

        The dest must have the following attributes:
        - "connection": name of the jdbc connection
        - "table": name of the table
        - "options": (optional) extra options to pass to the jdbc connector write method
        # https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

        The corresponding jdbc connection must have the following attributes:

        - type = 'jdbc'
        - protocol = 'sqlserver'
        - host = Server name
        - user = User name
        - password = Password
        - database = Database name

        :param df: the DataFrame to export
        :param dest: a dictionary describing the destination
        :return:
        """
        sql_url = self._get_url() + ";ApplicationIntent=ReadWrite"
        df.write.options(**dest.get("options", {})).jdbc(sql_url, dest["table"], dest["mode"])

    def test_connection(self):
        sql_url = self._get_url() + ";ApplicationIntent=ReadOnly"
        df = self.spark.read.jdbc(sql_url, "(SELECT 1 as ok) temp")
        if df.collect()[0].ok == 1:
            return True
        else:
            df.show()
            return False
