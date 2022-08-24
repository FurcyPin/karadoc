from typing import Any, Dict, Iterable, Optional, Tuple

from dynaconf.utils.boxing import DynaBox

from karadoc.common.conf import keyvault
from karadoc.common.utils.assert_utils import assert_true


class ConfBox:
    """Wrapper Class of Dynabox that allows to fetch values from secure vaults like Azure Keyvault"""

    def __init__(self, box: DynaBox, env: Optional[str] = None):
        """

        :param box: The DynaBox to wrap
        :param env: The env this ConfBox was built in
        """
        assert_true(isinstance(box, DynaBox))
        self.__box = box
        self.__env = env

    def __fetch_if_secret(self, value: Any) -> Any:
        if isinstance(value, dict) and "secret" in value:
            secret_env, secret_path = list(value.get("secret").items())[0]
            return keyvault.get_secret(conn_name=secret_env, secret_id=secret_path, env=self.__env)
        elif isinstance(value, DynaBox):
            return ConfBox(value, self.__env)
        else:
            return value

    def is_secret(self, item):
        value = self.__box.get(item)
        return isinstance(value, dict) and "secret" in value

    def __getattr__(self, item, *args, **kwargs):
        return self.__fetch_if_secret(self.__box.__getattr__(item, *args, **kwargs))

    def __getitem__(self, item, *args, **kwargs):
        return self.__fetch_if_secret(self.__box.__getitem__(item, *args, **kwargs))

    def __contains__(self, item):
        return self.__box.__contains__(item)

    def __getstate__(self):
        """Function used by pickle to serialize the class"""
        return self.__box

    def __setstate__(self, state):
        """Function used by pickle to deserialize the class"""
        self.__init__(state)

    def keys(self):
        return self.__box.keys()

    def items(self):
        for k, v in self.__box.items():
            yield k, self.__fetch_if_secret(v)

    def values(self):
        for k, v in self.__box.values():
            yield self.__fetch_if_secret(v)

    def get(self, item, default=None, *args, **kwargs):
        return self.__fetch_if_secret(self.__box.get(item, default, *args, **kwargs))

    def __str__(self) -> str:
        return str(self.__box.to_dict())

    def __repr__(self) -> str:
        return f"<ConfBox({repr(self.__box.to_dict())})>"

    def __eq__(self, other) -> bool:
        if isinstance(other, ConfBox):
            return self.__box == other.__box
        else:
            return NotImplemented

    def __to_flat_dict(self, prefix: str = "") -> Iterable[Tuple[str, object]]:
        for k, v in self.items():
            new_prefix = prefix
            if new_prefix != "":
                new_prefix = new_prefix + "."
            new_prefix += str(k)
            if isinstance(v, ConfBox):
                yield from v.__to_flat_dict(new_prefix)
            else:
                yield new_prefix, v

    def to_flat_dict(self) -> Dict[str, object]:
        """Returns a flat dictionary with all the {key: value} settings contained into this ConfBox

        Example:

        >>> conf = dict_to_dynabox({
        ...    "spark.driver.cores": 2,
        ...    "spark.driver.memory": "4g"
        ... })
        >>> conf_box = ConfBox(conf)
        >>> conf_box
        <ConfBox({'spark': {'driver': {'cores': 2, 'memory': '4g'}}})>
        >>> conf_box.to_flat_dict()
        {'spark.driver.cores': 2, 'spark.driver.memory': '4g'}

        """
        return {k: v for (k, v) in self.__to_flat_dict()}


def __unflatten_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a flat dictionary with keys containing dots (".") into a nested dictionary with keys split by "."

    >>> __unflatten_dict({'a': 'b'})
    {'a': 'b'}
    >>> __unflatten_dict({'a.b': 'c'})
    {'a': {'b': 'c'}}
    >>> __unflatten_dict({'a.b.c': 'd'})
    {'a': {'b': {'c': 'd'}}}
    >>> __unflatten_dict({'a': {'b.c': 'd'}})
    {'a': {'b': {'c': 'd'}}}

    :param d: a flat dict
    :return: a nested dict
    """
    res = {}
    for key, value in d.items():
        if isinstance(value, dict):
            value = __unflatten_dict(value)
        sub_keys = key.split(".")
        current = res
        for sub_key in sub_keys[:-1]:
            if sub_key not in current:
                current[sub_key] = {}
            current = current[sub_key]
        current[sub_keys[-1]] = value
    return res


def dict_to_dynabox(d):
    """Transforms a dict into a DynaBox. Mostly used for testing.

    >>> dict_to_dynabox({"a": {"b" : "c"}})
    <Box: {'a': {'b': 'c'}}>
    >>> dict_to_dynabox({"a": {"b" : "c"}}).get("a")
    <Box: {'b': 'c'}>
    >>> dict_to_dynabox({"a.b" : "c"})
    <Box: {'a': {'b': 'c'}}>
    >>> dict_to_dynabox({"a.b" : "c"}).get("a")
    <Box: {'b': 'c'}>

    :param d:
    :return:
    """
    if isinstance(d, dict):
        return DynaBox({k: dict_to_dynabox(v) for k, v in __unflatten_dict(d).items()})
    else:
        return d
