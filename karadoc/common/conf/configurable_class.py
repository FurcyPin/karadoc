import inspect
from typing import Callable, Dict, Iterable, Type

from karadoc.common.conf import ConfBox
from karadoc.common.exceptions import MissingConfigurationParameterException
from karadoc.common.validations import (
    ValidationResult,
    ValidationResultTemplate,
    ValidationSeverity,
)

ValidationResult_SecretParam = ValidationResultTemplate(
    check_type="karadoc.conf.secret_param",
    message_template="Parameter {param_name} should be secret",
    default_severity=ValidationSeverity.Warning,
)

ValidationResult_WrongParamType = ValidationResultTemplate(
    check_type="karadoc.conf.param_type",
    message_template="Incorrect parameter type for {param_name}, "
    "expected {expected_type} "
    "and got {actual_type} instead",
    default_severity=ValidationSeverity.Error,
)

ValidationResult_MissingRequiredParam = ValidationResultTemplate(
    check_type="karadoc.conf.missing_required_param",
    message_template="Missing required parameter {param_name}",
    default_severity=ValidationSeverity.Error,
)

ValidationResult_UnknownParam = ValidationResultTemplate(
    check_type="karadoc.conf.unknown_param",
    message_template="Unknown parameter {param_name}",
    default_severity=ValidationSeverity.Error,
)


class ConfParam:
    def __init__(
        self,
        required: bool = False,
        default: object = None,
        description: str = None,
        type: Type = None,
        secret: bool = False,
    ):
        """Declares a parameter of a `ConfigurableClass`

        :param required: indicates if the parameter is required (see method `ConfigurableClass.validate_params`)
        :param default: default value of the parameter when it is missing (ignored if required=True)
        :param description: description of the parameter
        :param type: type of the parameter (see method `ConfigurableClass.validate_params`)
        :param secret: indicates if the parameter should be stored in a secret store
        """
        self.required = required
        self.default = default
        self.description = description
        self.name = None
        self.type = type
        self.secret = secret
        self._parent = None

    def __str__(self) -> str:
        return (
            f"ConfParam(name={self.name}, type={self.type.__name__}, "
            f"required={self.required}, default={self.default}, secret={self.secret})"
        )

    def get(self, required: bool = None, default=None):
        """Returns the value of this parameter, fetched from the conf.

        Arguments may be passed to this method to override locally the settings of this parameter

        :param required: Locally overrides the defined value of "required" for this parameter
        :param default: Locally overrides the defined value of "default" for this parameter
        :return:
        """
        if default is not None:
            returned_default = default
        elif isinstance(self.default, ConfParam):
            returned_default = self.default.get()
        elif isinstance(self.default, Callable):
            returned_default = self.default(self._parent)
        else:
            returned_default = self.default
        if required is None:
            required = self.required
        if required:
            self.require()
        return self._parent.conf.get(self.name, returned_default)

    def require(self) -> None:
        """Raises an exception if this param is not specified in the configuration"""
        if self.name not in self._parent.conf:
            raise MissingConfigurationParameterException(param_name=self.name)

    def is_defined(self) -> bool:
        return self.name in self._parent.conf


class ConfigurableClass:
    """Abstract class that other classes may inherit to declare and use ConfParams.

    **Examples**
    *(more examples available in the documentation of the `validate_params` method)*

    >>> class MyClass(ConfigurableClass):  # noqa: E501
    ...    a = ConfParam()                                        # A: Parameter values may be passed via at class instantiation via a dict-like conf
    ...    b = ConfParam()                                        # B: Missing parameters that are not required will be None
    ...    c = ConfParam(default="c")                             # C: Parameters may have a default value
    ...    d = ConfParam(default=a)                               # D: The default value can be another parameter
    ...    e = ConfParam(default=lambda x: x.a.get() + x.c.get()) # E: The default value can be the result of a combination of other parameters
    ...
    ...    def __init__(self, connection_conf):
    ...        super().__init__(connection_conf)
    >>> my_class = MyClass({'a': 'a'})
    >>> list(my_class.validate_params()) # Call this for immediate validation
    []
    >>> my_class.a.get() # A: Parameter values may be passed via at class instantiation via a dict-like conf
    'a'
    >>> my_class.b.get() is None # B: Missing parameters that are not required will be None
    True
    >>> my_class.c.get() # C: Parameters may have a default value
    'c'
    >>> my_class.c.get(default='z') # C: The default value may be locally overridden
    'z'
    >>> my_class.c.get(required=True) # C: Param may be made required locally
    Traceback (most recent call last):
      ...
    karadoc.common.exceptions.package.MissingConfigurationParameterException: Missing required parameter c
    >>> my_class.d.get() # D: The default value can be another parameter
    'a'
    >>> my_class.e.get() # E: The default value can be the result of a combination of other parameters
    'ac'
    """

    def __init__(self, conf: Dict):
        """
        :param conf: a nested dict-like object (a dict of dict, a parsed json, a dynaconf's DynaBox, etc.)
        """
        self.conf = conf
        self.params: Dict[str, ConfParam] = {
            name: param for name, param in inspect.getmembers(self) if isinstance(param, ConfParam)
        }
        for name, param in self.params.items():
            param.name = name
            param._parent = self

    def validate_params(self) -> Iterable[ValidationResult]:
        """Validates the correctness of all parameters passed to the inheriting class.
        This method may be called immediately in the `__init__()` call of the inheriting class for immediate validation,
        our later for lazy evaluation, depending on the use cases.

        Validation performed:
        - Required params must be present in the passed configuration (even if they have a default value)
        - The type of each param in the passed configuration must match the type in the declaration, when it is declared

        >>> class MyClass(ConfigurableClass):
        ...    a = ConfParam(type=int)                  # A: Parameters should have the correct declared type
        ...    b = ConfParam(required=True)             # B: required parameters must have a value
        ...    c = ConfParam(required=True, default=3)  # C: default values are useless for required parameters
        ...
        ...    def __init__(self, connection_conf):
        ...        super().__init__(connection_conf)
        >>> my_class_1 = MyClass({'b': 2, 'c': 2})
        >>> my_class_1.conf
        {'b': 2, 'c': 2}
        >>> list(my_class_1.validate_params())
        []
        >>> my_class_2 = MyClass({'a': 'a', 'z': 'z'}) # E: passed variables should have the correct declared type
        >>> for res in my_class_2.validate_params(): print(res)
        Error: karadoc.conf.param_type: Incorrect parameter type for a, expected int and got str instead
        Error: karadoc.conf.missing_required_param: Missing required parameter b
        Error: karadoc.conf.missing_required_param: Missing required parameter c
        Error: karadoc.conf.unknown_param: Unknown parameter z

        """
        for name, param in self.params.items():
            yield from _check_secret_params(name, self.conf, param)
            yield from _check_param_types(name, self.conf, param)
            yield from _check_required_param(name, self.conf, param)
        for name in self.conf.keys():
            yield from _check_unknown_param(name, self.params)


def __is_secret_conf(conf: dict, name: str):
    return isinstance(conf, ConfBox) and conf.is_secret(name)


def _check_secret_params(name: str, conf: Dict, param: ConfParam) -> Iterable[ValidationResult]:
    if name in conf and param.secret and not __is_secret_conf(conf, name):
        yield ValidationResult_SecretParam(param_name=name)


def _check_param_types(name: str, conf: Dict, param: ConfParam) -> Iterable[ValidationResult]:
    if name in conf and param.type is not None and not __is_secret_conf(conf, name):
        value = conf.get(name)
        if not isinstance(value, param.type):
            yield ValidationResult_WrongParamType(
                param_name=name, expected_type=param.type.__name__, actual_type=type(value).__name__
            )


def _check_required_param(name: str, conf: Dict, param: ConfParam) -> Iterable[ValidationResult]:
    if name not in conf and param.required:
        yield ValidationResult_MissingRequiredParam(param_name=name)


def _check_unknown_param(name, params: Dict[str, ConfParam]):
    if name not in params:
        yield ValidationResult_UnknownParam(param_name=name)
