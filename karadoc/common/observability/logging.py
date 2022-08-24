import importlib
import logging
import sys
from logging import Formatter, StreamHandler
from typing import Optional

from karadoc.common.class_utils import find_class_from_module
from karadoc.common.conf import ConfBox
from karadoc.common.observability import LogEvent
from karadoc.common.observability.console_event import ConsoleEvent
from karadoc.common.observability.logging_utils import add_logging_level

VERBOSE = False
DEFAULT_CONSOLE_LOG_LEVEL = "USER_OUTPUT"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
DEFAULT_VERBOSITY = False


class SimpleConsoleFormatter(Formatter):
    """Formatter used by the stdout_handler to format ConsoleEvents in a more user-friendly way."""

    def __init__(self, fmt=None, datefmt=None, style="%", verbose: bool = DEFAULT_VERBOSITY):
        super().__init__(fmt, datefmt, style)
        self.verbose = verbose

    def format_exc_and_stack(self, record: logging.LogRecord):
        s = ""
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + record.exc_text
        if record.stack_info:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + self.formatStack(record.stack_info)
        return s

    def format(self, record: logging.LogRecord):
        if isinstance(record.msg, ConsoleEvent) and not (self.verbose or VERBOSE):
            console_event = record.msg
            s = console_event.message
            if console_event.extra_console_message:
                s += " " + console_event.extra_console_message
            s += self.format_exc_and_stack(record)
            return s
        elif isinstance(record.msg, LogEvent) and not (self.verbose or VERBOSE):
            return record.msg.message
        else:
            return super(SimpleConsoleFormatter, self).format(record)


def __clear_log_handlers():
    """Remove all log handlers"""
    logging_handlers = list(logging.root.handlers)
    for lh in logging_handlers:
        logging.root.removeHandler(lh)
        lh.close()


def __add_stdout_handler(observability_conf: Optional[ConfBox]):
    """Add handler to write logs on stdout"""
    level = DEFAULT_CONSOLE_LOG_LEVEL
    verbose = DEFAULT_VERBOSITY
    if observability_conf is not None:
        level = observability_conf.get("level", DEFAULT_CONSOLE_LOG_LEVEL).upper()
        verbose = observability_conf.get("verbose", DEFAULT_VERBOSITY)

    stdout_handler = StreamHandler(stream=sys.stdout)

    stdout_handler.setFormatter(SimpleConsoleFormatter(fmt=DEFAULT_LOG_FORMAT, verbose=verbose))

    logging.root.addHandler(stdout_handler)
    stdout_handler.setLevel(DEFAULT_CONSOLE_LOG_LEVEL)
    stdout_handler.setLevel(level)


def _load_observability_handler(handler_conf: ConfBox):
    handler_module = handler_conf.get("type")
    module = importlib.import_module(handler_module)
    handler_class = find_class_from_module(module, logging.Handler)
    azure_handler: logging.Handler = handler_class(**{k: v for k, v in handler_conf.items()})
    level = handler_conf.get("level", DEFAULT_LOG_LEVEL).upper()
    azure_handler.setLevel(level)
    logging.root.addHandler(azure_handler)


def _load_observability_handlers(observability_conf: ConfBox):
    if observability_conf is not None:
        for handler_name, handler_conf in observability_conf.items():
            _load_observability_handler(handler_conf)


def __apply_conf(observability_conf: Optional[ConfBox]) -> None:
    """Apply an observability configuration, if provided"""
    if observability_conf is not None:
        level = observability_conf.get("level", DEFAULT_LOG_LEVEL).upper()
        logging.root.setLevel(level)
        _load_observability_handlers(observability_conf.get("handler"))


def setup_logging(observability_conf: Optional[ConfBox], verbose: Optional[bool]) -> None:
    """Initialize logging with the given configuration.
    We add a new logging level called USER_OUTPUT to avoid flooding the user with INFO-level logs.

    :param observability_conf: dynaconf section related to observability
    :param verbose: Activates verbose mode
    """
    global VERBOSE
    if verbose is not None:
        VERBOSE = verbose
    add_logging_level(level_name="USER_OUTPUT", level_num=logging.INFO + 5)
    __clear_log_handlers()
    __add_stdout_handler(observability_conf)
    logging.root.setLevel(DEFAULT_LOG_LEVEL)
    __apply_conf(observability_conf)
