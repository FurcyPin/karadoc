from typing import Dict, List, Set, Union

from pyspark import Row
from pyspark.sql import DataFrame

from karadoc.common import spark_utils
from karadoc.spark.utils import get_spark_session


def _compare_list_ignore_order(expected: Set, actual: List) -> bool:
    if len(expected) != len(actual):
        return False
    for x in expected:
        if x not in actual:
            return False
    return True


def _compare_dicts(expected: Dict, actual: Dict) -> bool:
    if len(expected) != len(actual):
        return False
    for k, expected_v in expected.items():
        if k not in actual:
            return False
        actual_v = actual[k]
        if isinstance(expected_v, Set):
            if not _compare_list_ignore_order(expected_v, actual_v):
                return False
        elif isinstance(actual_v, Set):
            if not _compare_list_ignore_order(actual_v, expected_v):
                return False
        elif expected_v != actual_v:
            return False
    return True


class MockRow:
    """This class can be used in unit tests to be easily compared with a real Spark Row

    Ignoring order for ARRAY comparison
    ----------------------------------
    When testing DataFrames with columns of type ARRAY, one might want to make their test cases ignore the ordering
    of the arrays.

    This can be done be declaring values as sets instead of lists.
    List values will check that the order matches while set values will ignore ordering

    Warning: two MockRows cannot be compared together.
    This is to prevent sets like `{MockRow(a=1), MockRow(a=1)}` to be reduced to `{MockRow(a=1)}`.

    Example:

    >>> Row(a=[1, 2, 3]) == MockRow(a=[1, 2, 3])
    True
    >>> Row(a=[3, 2, 1]) == MockRow(a=[1, 2, 3])
    False

    >>> Row(a=[1, 2, 3]) == MockRow(a={1, 2, 3})
    True
    >>> Row(a=[3, 2, 1]) == MockRow(a={1, 2, 3})
    True

    """

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __hash__(self):
        return hash((tuple(self.kwargs.keys()), self.kwargs.values()))

    def __eq__(self, other):
        if isinstance(other, Row):
            return _compare_dicts(self.kwargs, other.asDict())
        else:
            return NotImplemented

    def __repr__(self):
        return f"MockRow({self.kwargs})"

    def to_row(self) -> Row:
        """Transforms this MockRow into a regular pyspark.sql.Row

        >>> MockRow(a=[1, 2, 3]).to_row()
        Row(a=[1, 2, 3])
        >>> MockRow(a=[1, 2, 3]).to_row()
        Row(a=[1, 2, 3])

        >>> MockRow(a={1, 2, 3}).to_row()
        Row(a=[1, 2, 3])
        >>> MockRow(a={1, 2, 3}).to_row()
        Row(a=[1, 2, 3])

        """

        def to_row(r):
            if isinstance(r, MockRow):
                return r.to_row()
            if isinstance(r, list) or isinstance(r, set):
                return [to_row(x) for x in r]
            else:
                return r

        return Row(**{k: to_row(v) for k, v in self.kwargs.items()})


class MockDataFrame:
    """This class can be used in unit tests to be easily compared with a real Spark DataFrame.

    Ignoring row ordering
    ---------------------
    When comparing DataFrames with multiple rows, the order of rows can be ignored by declaring a set of rows instead
    of a list.


    Example:

    >>> spark = spark_utils.get_spark_session("doctest")
    >>> spark.createDataFrame([Row(a=1), Row(a=2)]) == MockDataFrame([MockRow(a=1), MockRow(a=2)])
    True
    >>> spark.createDataFrame([Row(a=1), Row(a=2)]) == MockDataFrame([MockRow(a=2), MockRow(a=1)])
    False
    >>> spark.createDataFrame([Row(a=1), Row(a=2)]) == MockDataFrame({MockRow(a=1), MockRow(a=2)})
    True
    >>> spark.createDataFrame([Row(a=1), Row(a=2)]) == MockDataFrame({MockRow(a=2), MockRow(a=1)})
    True
    >>> spark.createDataFrame([Row(a=1), Row(a=1)]) == MockDataFrame({MockRow(a=1), MockRow(a=1)})
    True

    """

    def __init__(self, rows: Union[Set[MockRow], List[MockRow]]):
        self.rows = rows

    def __eq__(self, other):
        if isinstance(other, DataFrame):
            other = other.select(spark_utils.quote_columns(sorted(other.columns)))
            other_rows = other.collect()
            if isinstance(self.rows, Set):
                return _compare_list_ignore_order(self.rows, other_rows)
            else:
                return other_rows == self.rows
        else:
            return NotImplemented

    def __repr__(self):
        return f"MockDataFrame({self.rows})"

    def to_dataframe(self) -> DataFrame:
        """Transforms this MockDataFrame into a regular pyspark.sql.DataFrame

        >>> spark = spark_utils.get_spark_session("doctest")
        >>> MockDataFrame([MockRow(a=1, b=[2], c={3})]).to_dataframe().show()
        +---+---+---+
        |  a|  b|  c|
        +---+---+---+
        |  1|[2]|[3]|
        +---+---+---+
        <BLANKLINE>

        >>> MockDataFrame({MockRow(a=1, b=[2], c={3})}).to_dataframe().show()
        +---+---+---+
        |  a|  b|  c|
        +---+---+---+
        |  1|[2]|[3]|
        +---+---+---+
        <BLANKLINE>

        :return:
        """
        spark = get_spark_session()
        return spark.createDataFrame([r.to_row() for r in self.rows])
