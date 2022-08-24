import logging
import sys  # noqa: F401 (used only in doctest)
from logging import Formatter, StreamHandler  # noqa: F401 (used only in doctest)
from typing import Optional


def add_logging_level(level_name: str, level_num: int, method_name: Optional[str] = None):
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `levelName` becomes an attribute of the `logging` module with the value
    `levelNum`. `methodName` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `methodName` is not specified, `levelName.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    Example
    -------
    >>> add_logging_level('TRACE', logging.DEBUG - 5)
    >>> LOG = logging.getLogger(__name__)
    >>> LOG.setLevel('TRACE')
    >>> stdout_handler = StreamHandler(stream=sys.stdout)
    >>> stdout_handler.setFormatter(Formatter(fmt="%(message)s"))
    >>> LOG.addHandler(stdout_handler)
    >>> LOG.propagate = False # This prevents the message from being logged by handlers added to the root logger.
    >>> LOG.trace('this works')
    this works

    """
    # This method was taken from this Stack Overflow answer:
    # https://stackoverflow.com/q/2183233/35804945#35804945
    if hasattr(logging, level_name):
        return
    if not method_name:
        method_name = level_name.lower()

    if hasattr(logging, level_name):
        raise AttributeError("{} already defined in logging module".format(level_name))
    if hasattr(logging, method_name):
        raise AttributeError("{} already defined in logging module".format(method_name))
    if hasattr(logging.getLoggerClass(), method_name):
        raise AttributeError("{} already defined in logger class".format(method_name))

    def log_for_level(self, message, *args, **kwargs):
        if self.isEnabledFor(level_num):
            self._log(level_num, message, args, **kwargs)

    def log_to_root(message, *args, **kwargs):
        logging.log(level_num, message, *args, **kwargs)

    logging.addLevelName(level_num, level_name)
    setattr(logging, level_name, level_num)
    setattr(logging.getLoggerClass(), method_name, log_for_level)
    setattr(logging, method_name, log_to_root)
