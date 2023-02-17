from unittest import TestCase

from karadoc.spark.conf import get_spark_conf
from karadoc.spark.utils import get_spark_session
from karadoc.test_utils.mock_settings import mock_settings_for_test, mock_settings_for_test_class


@mock_settings_for_test_class({"spark.conf": {"spark.sql.shuffle.partitions": 9}})
class TestConf(TestCase):
    def test_get_spark_conf_default(self):
        """When running tests, Spark configuration defined in conf/spark/settings.toml for the "development"
        environment should be taken into account and override the "default" environment.
        """
        self.assertEqual(get_spark_conf().get("spark.driver.memory"), "3g")
        self.assertEqual(get_spark_conf().get("spark.executor.memory"), "3g")

    def test_get_spark_conf_with_mock_settings_for_test_class(self):
        """When running tests, configuration overrides made with mock_settings_for_test_class
        should be taken into account.

        in conf/spark/settings.toml, we have:

            [development.spark.conf]
              spark.sql.shuffle.partitions = 10

        but we also have this decorator at the class level:

            @mock_settings_for_test_class({"spark.conf": {'spark.sql.shuffle.partitions': 9}})

        which overrides it for the class
        """
        print(get_spark_conf())
        self.assertEqual(get_spark_conf().get("spark.sql.shuffle.partitions"), 9)

    @mock_settings_for_test({"spark.conf": {"spark.sql.shuffle.partitions": 8}})
    def test_get_spark_conf_with_mock_settings_for_test(self):
        """When running tests, configuration overrides made with mock_settings_for_test_class
        should be taken into account.

        in conf/spark/settings.toml, we have:

            [development.spark.conf]
              spark.sql.shuffle.partitions = 10

        and we have this decorator at the class level:

            @mock_settings_for_test_class({"spark.conf": {'spark.sql.shuffle.partitions': 9}})

        but we also have this decorator at the test level:

            @mock_settings_for_test({"spark.conf": {'spark.sql.shuffle.partitions': 8}})

        which overrides it for the class
        """
        self.assertEqual(get_spark_conf().get("spark.sql.shuffle.partitions"), 8)

    def test_get_spark_conf_with_extra_spark_conf(self):
        """When running tests, configuration overrides made with mock_settings_for_test_class
        should be taken into account

        :return:
        """
        spark1 = get_spark_session(None, extra_spark_conf={"spark.sql.shuffle.partitions": 20})
        self.assertEqual(spark1.conf.get("spark.sql.shuffle.partitions"), "20")

        spark2 = get_spark_session(None, extra_spark_conf={"spark.sql.shuffle.partitions": 30})
        self.assertEqual(spark2.conf.get("spark.sql.shuffle.partitions"), "30")

        self.assertEqual(spark1, spark2)
