from typing import List


class MissingConfigurationException(Exception):
    """Exception raised when we fail to load a configuration that has no default value"""

    def __init__(self, conf_id: str) -> None:
        msg = f"Could not find configuration {conf_id}\n"
        "Please make sure your environment variable KARADOC_ENV is correctly set"
        Exception.__init__(self, msg)


class MissingConfigurationParameterException(Exception):
    """Exception raised when a required parameter is missing from a ConfigurableClass"""

    def __init__(self, param_name: str) -> None:
        msg = f"Missing required parameter {param_name}"
        Exception.__init__(self, msg)


class MissingVariablesException(Exception):
    """Exception raised when a job requiring variables is run without specifying variable values"""

    def __init__(self, missing_keys: List[str]) -> None:
        missing_keys_str = ", ".join(missing_keys)
        msg = f"The following variables are required and missing: [{missing_keys_str}]"
        Exception.__init__(self, msg)


class JobDisabledException(Exception):
    def __init__(self) -> None:
        msg = "The job has been disabled, it should not be launched"
        Exception.__init__(self, msg)


class ActionFileLoadingError(Exception):
    """Error raised when failing to laod an Action File"""

    def __init__(self, msg: str) -> None:
        Exception.__init__(self, msg)


class CommandValidationError(Exception):
    """Error raised when a command fails validation"""

    def __init__(self, msg: str) -> None:
        Exception.__init__(self, msg)


class ConnectorLoadError(Exception):
    """Error raised when a failing to load a connector"""

    def __init__(self, msg: str) -> None:
        Exception.__init__(self, msg)


class LoggedException(Exception):
    """Exception raised when an exception has already been caught and logged"""

    def __init__(self) -> None:
        Exception.__init__(self, "See parent exception")


class IllegalJobInitError(Exception):
    """Error raised when job.init() is called at an inappropriate time"""


class ForbiddenActionError(Exception):
    """Error raised when something that should not happen does happen"""
