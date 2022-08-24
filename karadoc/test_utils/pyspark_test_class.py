import logging
from unittest import TestCase

from karadoc.spark.utils import get_spark_session


class PySparkTest(TestCase):
    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.WARN)

    @classmethod
    def __create_testing_pyspark_session(cls):
        return get_spark_session()

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.__create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
