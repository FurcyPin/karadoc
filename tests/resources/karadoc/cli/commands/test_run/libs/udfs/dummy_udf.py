from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(StringType())
def dummy_udf(x):
    """We encountered weird import bugs with when a Spark UDF was importing another function"""
    return dummy_udf2(x)


def dummy_udf2(x):
    return x
