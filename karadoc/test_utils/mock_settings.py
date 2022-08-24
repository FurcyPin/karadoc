from unittest import mock

from dynaconf import settings
from dynaconf.utils.boxing import DynaBox

from karadoc.common.conf.conf_box import dict_to_dynabox

original_spark_settings = dict_to_dynabox({"spark": {"conf": settings.get("spark.conf")}})


# Explanation
# -----------
# This method is necessary to simulate dynaconf's slightly weird behavior.
#
# For instance, if I have the following settings.toml
#
#       [development]
#       a.b.c = "x"
#
# And if I run the following code:
#
#       from dynaconf import settings
#
#       print(settings.get("a").get("b").get("c") == "x")
#       # True
#       print(settings.get("a.b").get("c") == "x")
#       # True
#       print(settings.get("a").get("b.c") == None)
#       # True
#
# We notice that the keys are split by "." at the root level (settings) but not at the subsequent levels (Dynabox)
def _split_get(box, key: str):
    """Get a key from a nested dict-like object by splitting the key according to "."

    >>> b = {"a": {"b": 2}}
    >>> _split_get(b, "a")
    {'b': 2}
    >>> _split_get(b, "a.b")
    2
    >>> _split_get(b, "c")

    >>> _split_get(b, "a.c")

    >>> _split_get(b, "c.c")

    >>> _split_get({}, "a")

    :param box: A nested-dict-like object
    :param key: A string key
    :return: the value located at entry key
    """
    split_key = key.split(".")
    res = box
    for k in split_key:
        res = res.get(k)
        if res is None:
            return None
    return res


def __merge_2_boxes(box1: DynaBox, box2: DynaBox) -> DynaBox:
    """Merges two DynaBox together

    >>> box1 = dict_to_dynabox({"a": {"b": 2}})
    >>> box2 = dict_to_dynabox({"a": {"b": 3, "c": 3}})
    >>> __merge_2_boxes(box1, box2)
    <Box: {'a': {'b': 2, 'c': 3}}>

    :param box1:
    :param box2:
    :return:
    """
    # We only need the keys, but we use a dict instead of a set because dicts preserve insertion order on keys
    keys = {**box1, **box2}
    res = {}
    for key, _ in keys.items():
        v1 = box1.get(key)
        v2 = box2.get(key)
        if isinstance(v1, DynaBox) and isinstance(v2, DynaBox):
            res[key] = merge_boxes(v1, v2)
        elif key in box1:
            res[key] = v1
        else:
            res[key] = v2
    return dict_to_dynabox(res)


def merge_boxes(box: DynaBox, *boxes: DynaBox) -> DynaBox:
    """Recursively merge multiple DynaBoxes together
    The left-most boxes take precedence.

    >>> box1 = dict_to_dynabox({"a": {"b": 2}})
    >>> box2 = dict_to_dynabox({"a": {"b": 3, "c": 3}})
    >>> box3 = dict_to_dynabox({"d": 4})
    >>> merge_boxes(box1, box2, box3)
    <Box: {'a': {'b': 2, 'c': 3}, 'd': 4}>

    :param box: The first box (we need at least one argument)
    :param boxes: The other boxes
    :return:
    """
    res = box
    for b in boxes:
        res = __merge_2_boxes(res, b)
    return res


def _coalesce_box_get(*boxes: DynaBox):
    """Returns a "get(key, default)" method that returns the value found in the first dictionary that contains
    that key, or default otherwise.
    If the returned value is a DynaBox, it is merged with the values found in the other DynaBoxes.


    This also splits the key on "." and recursively look for nested entries.

    >>> box1 = dict_to_dynabox({"a": {"b": 2}})
    >>> box2 = dict_to_dynabox({"a": {"b": 3, "c": 3}})
    >>> box3 = dict_to_dynabox({"d": 4})
    >>> cget = _coalesce_box_get(box1, box2, box3)
    >>> cget("a.b")
    2

    The first non-null value matching the key is returned
    >>> cget("a.c")
    3

    Except for values of type Dict, which are merged for the whole dicts and returned as a DynaBox
    >>> cget("a")
    <Box: {'b': 2, 'c': 3}>
    >>> cget("d")
    4
    >>> cget("a.missing", 0)
    0

    When merging two values of incompatible type, a TypeError is raised
    >>> box1 = dict_to_dynabox({"a": {"b": 2}})
    >>> box2 = dict_to_dynabox({"a": 2})
    >>> cget = _coalesce_box_get(box1, box2, box3)
    >>> cget("a")
    Traceback (most recent call last):
      ...
    TypeError: key a matches values of incompatible types: {'b': 2} and 2

    :param dicts:
    :return:
    """

    def get(key: str, default=None):
        """Return the value in the first dictionary that contains this key.

        :param key:
        :param default:
        :return:
        """
        res = None
        dict_res = {}
        for d in boxes:
            res = _split_get(d, key)
            if res is not None:
                if isinstance(res, DynaBox):
                    dict_res = merge_boxes(dict_res, res)
                elif dict_res != {}:
                    raise TypeError(f"key {key} matches values of incompatible types: {dict_res} and {res}")
                else:
                    return res
        if dict_res != {}:
            return dict_to_dynabox(dict_res)
        else:
            return default

    return get


def _override_for_test_class(box: DynaBox, key, default):
    return _coalesce_box_get(box, original_spark_settings)(key, default)


def mock_settings_for_test_class(settings_dict=None):
    """Method used to override Karadoc's settings for testing purposes:

    You can use it as mock in unit tests or directly override the settings in a Python Console.

    Example :

        .. code-block:: python

            from unittest import TestCase
            from unittest import mock
            @mock_settings_for_test_class({'model_dir': 'tests/resources/model_graph'})
            class TestSomething(TestCase):
                ...

    :param settings_dict:
    :return:
    """
    if settings_dict is None:
        settings_dict = {}
    settings_box = dict_to_dynabox(settings_dict)

    def override(key, default=None):
        return _override_for_test_class(settings_box, key, default)

    return mock.patch("dynaconf.settings.get", override)


def mock_settings_for_test(settings_dict):
    """Method used to override Karadoc's settings for testing purposes:

    You can use it as mock in unit tests or directly override the settings in a Python Console.

    Example as unit test :

    Example :

        .. code-block:: python

            from unittest import TestCase
            from unittest import mock
            @mock_settings_for_test_class()
            class TestSomething(TestCase):

                @mock_settings_for_test({'allow_missing_vars': True})
                def test_something():
                ...

    :param settings_dict:
    :return:
    """
    settings_box = dict_to_dynabox(settings_dict)

    def override(original_box, key, default):
        return _coalesce_box_get(settings_box, original_box, original_spark_settings)(key, default)

    return mock.patch("karadoc.test_utils.mock_settings._override_for_test_class", override)
