import ast
import re
from datetime import datetime, timedelta
from typing import Dict, List

from karadoc.common.utils.assert_utils import assert_true

DATE_FORMAT = "%Y-%m-%d"


def day_range(start_date: str, end_date: str, step: int = 1) -> List[str]:
    """Compute the interval of days [start_date, end_date[
    >>> day_range('2019-10-01', '2019-10-07', 1)
    ['2019-10-01', '2019-10-02', '2019-10-03', '2019-10-04', '2019-10-05', '2019-10-06']
    >>> day_range('2019-10-01', '2019-10-31', 7)
    ['2019-10-01', '2019-10-08', '2019-10-15', '2019-10-22', '2019-10-29']

    :param start_date: Left bound of the interval (included)
    :param end_date: Right bound of the interval (not included). Should be greater or equal to start_date
    :param step: (default: 1) Number of days between each element
    :return:
    """
    assert_true(step > 0, "day_range() arg step should be greater than zero")
    start_datetime = datetime.strptime(start_date, DATE_FORMAT)
    end_datetime = datetime.strptime(end_date, DATE_FORMAT)
    numdays = (end_datetime - start_datetime).days
    assert_true(start_datetime <= end_datetime, "day_range() arg end_date should be greater or equal to start_date")
    return [(start_datetime + timedelta(days=x)).strftime(DATE_FORMAT) for x in range(0, numdays, step)]


def expand_vars(vars: Dict[str, str]) -> List[Dict[str, str]]:
    """

    >>> vars = { "day_1": "day_range('2019-10-01', '2019-10-03')", "day_2": "day_range('2020-01-01', '2020-01-03')"}
    >>> for v in expand_vars(vars):
    ...     print(v)
    {'day_1': '2019-10-01', 'day_2': '2020-01-01'}
    {'day_1': '2019-10-01', 'day_2': '2020-01-02'}
    {'day_1': '2019-10-02', 'day_2': '2020-01-01'}
    {'day_1': '2019-10-02', 'day_2': '2020-01-02'}

    :param vars:
    :return:
    """
    res = [vars.copy()]
    for key, value in vars.items():
        values = [value]
        match = next(re.finditer(r"^day_range(?P<args>\(.*\))$", value), None)
        if match is not None:
            args = match.group("args")
            values = day_range(*ast.literal_eval(args))
        new_res = []
        for r in res:
            for v in values:
                new_r = r.copy()
                new_r[key] = v
                new_res.append(new_r)
        res = new_res
    return res
