from pprint import pprint  # noqa: F401 (used only in doctest)
from typing import Any, Dict, Iterable


def pretty_format(obj: object, indent: int = 2) -> str:
    """Pretty prints an object serialized as json.
    We don't use the pprint method because the output it does not display Dict[Dict[str]] as we want:

    Examples: It must look prettier than pprint
    >>> my_dict={'a':1, 's':{'b': 2, 'c': [3, 4]}}
    >>> pprint(my_dict, indent=2, width=1)
    { 'a': 1,
      's': { 'b': 2,
             'c': [ 3,
                    4]}}
    >>> print(pretty_format(my_dict))
    {
      "a": 1,
      "s": {
        "b": 2,
        "c": [3, 4]
      }
    }

    It must split long lists:
    >>> my_dict={'a':1, 's':{'b': 2, 'c': [33333333333333333333333333333, 44444444444444444444444444444444]}}
    >>> print(pretty_format(my_dict))
    {
      "a": 1,
      "s": {
        "b": 2,
        "c": [
          33333333333333333333333333333,
          44444444444444444444444444444444
        ]
      }
    }

    It must support non-serializable objects:
    >>> from datetime import datetime
    >>> my_dict={'a':1, 's':{'b': 2, 'c': datetime.strptime("2021-10-28", '%Y-%m-%d')}}
    >>> print(pretty_format(my_dict))
    {
      "a": 1,
      "s": {
        "b": 2,
        "c": 2021-10-28 00:00:00
      }
    }

    :param obj:
    :param indent:
    :return:
    """

    def format_list(iterable: Iterable[Any], prefix: str) -> str:
        new_prefix = prefix + " " * indent
        items = [format_obj(item) for item in iterable]
        res = "[" + (", ").join(items) + "]"
        if len(res) > 50:
            res = "[\n" + new_prefix + (",\n" + new_prefix).join(items) + "\n" + prefix + "]"
        return res

    def format_dict(d: Dict[Any, Any], prefix: str) -> str:
        new_prefix = prefix + " " * indent
        items = [format_obj(k, new_prefix) + ": " + format_obj(v, new_prefix) for k, v in d.items()]
        return "{\n" + new_prefix + (",\n" + new_prefix).join(items) + "\n" + prefix + "}"

    def format_obj(o: Any, prefix: str = "") -> str:
        if isinstance(o, str):
            return '"' + o + '"'
        if isinstance(o, Dict):
            return format_dict(o, prefix)
        if isinstance(o, Iterable):
            return format_list(o, prefix)
        return str(o)

    return format_obj(obj)
