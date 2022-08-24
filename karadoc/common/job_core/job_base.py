from typing import Optional

from karadoc.common.utils.assert_utils import assert_true


class JobBase:
    _action_file_name_conf_key: str
    _run_method_name: Optional[str] = None
    output: str

    @classmethod
    def get_action_file_name(cls) -> str:
        from karadoc.common import conf

        assert_true(
            (cls._action_file_name_conf_key is not None),
            "_action_file_name variable must be overwritten by the class that extends JobBase",
        )
        return conf.get_action_file_name(cls._action_file_name_conf_key)
