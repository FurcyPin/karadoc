from distutils.util import strtobool
from typing import Dict, Optional

from karadoc.common import conf
from karadoc.common.exceptions import MissingVariablesException
from karadoc.common.utils.assert_utils import assert_true


class HasVars:
    """When vars are passed via the --vars option in the command line,
    we have to set them globally to let the Job object override it's own variables with it.
    """

    _global_vars: Optional[Dict[str, str]] = None
    __supported_types = [str, bool, int, float]
    __supported_types_names = [__cls.__name__ for __cls in __supported_types]

    def __init__(self) -> None:
        # Attributes that the user is not supposed to change
        self.__vars_override = HasVars._global_vars
        self.__vars: Dict[str, str] = dict()

    @property
    def vars(self) -> Dict[str, str]:
        """Variables that can be used inside the job and can be overwritten with the '--vars' option"""
        return self.__vars

    @vars.setter
    def vars(self, new_vars: Dict[str, str]):
        """Setting vars in a ACTION_FILE behave differently depending on the context.

        - When a job object is created directly in an interactive shell or in a ACTION_FILE loaded
          with the `load_non_runnable_action_file` methods, `global_vars` is None and the vars are set normally.

        - When a job object is created in an ACTION_FILE loaded with the `load_runnable_action_file` methods
          `global_vars` is not None and the vars are overridden by them. When `job.vars = {...}` is called
          inside the ACTION_FILE, we will not set them but only check that the global_vars correspond to the
          variables declared in the ACTION_FILE.

            When we run a job with declared variables (job.vars), the values of the declared variables
          must be passed via the --vars option.
          This security can be disabled on an environment by setting allow_missing_vars = true (default: false),
          in which case the default values will be used when missing, instead of raising an error.
          It can be useful for development environment, but it is recommended to keep this to false in production.

        :return:
        """
        self.__check_vars_type(new_vars)
        if self.__vars_override is None:
            self.__vars = new_vars
        else:
            if not conf.allow_missing_vars():
                self.__check_vars(new_vars, self.__vars_override)
            self.__vars = {k: self.__change_var_type(self.__vars_override.get(k, v), v) for k, v in new_vars.items()}

    @staticmethod
    def __change_var_type(value: str, template_value: object):
        """Cast the given value to the same type of the template_value.

        Only the following types are supported for now:
        - str
        - bool
        - int
        - float

        """
        if isinstance(template_value, str):
            return value
        elif isinstance(template_value, bool):
            assert_true(
                isinstance(value, str) or isinstance(value, bool),
                f"Expected str or bool, got {type(value).__name__} instead.",
            )
            if isinstance(value, str):
                return strtobool(value)
            else:
                return value
        elif isinstance(template_value, int):
            return int(value)
        elif isinstance(template_value, float):
            return float(value)
        else:
            raise TypeError(
                f"Type {type(template_value).__name__} is not supported for variables. "
                f"Only the following types are supported ${HasVars.__supported_types_names}"
            )

    @staticmethod
    def __check_vars(declared_vars: dict, provided_vars: dict):
        """Check that the all the declared variables are declared in the provided variables"""
        missing_keys = []
        for key in declared_vars.keys():
            if not provided_vars.__contains__(key):
                missing_keys.append(key)
        if len(missing_keys) > 0:
            raise MissingVariablesException(missing_keys=missing_keys)

    @staticmethod
    def __is_var_type_supported(value: object) -> bool:
        """Return true if the type of the given variable is supported"""
        for tpe in HasVars.__supported_types:
            if isinstance(value, tpe):
                return True
        return False

    @staticmethod
    def __check_vars_type(vars: Dict[str, object]):
        """Check that the type of the given variables is supported"""
        for key, value in vars.items():
            if not isinstance(key, str):
                raise ValueError(
                    f"Only strings are allowed as vars keys. Got key: {key} of type: {type(key).__name__} instead."
                )
            if not HasVars.__is_var_type_supported(value):
                raise ValueError(
                    f"Type {type(value).__name__} of variable '{key}' is not supported for variables. "
                    f"Only the following types are supported {HasVars.__supported_types_names}"
                )
