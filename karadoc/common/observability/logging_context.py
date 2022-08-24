import copy
import getpass
import threading
import uuid
from dataclasses import dataclass
from typing import Dict, Optional

from karadoc.common import conf
from karadoc.common.conf import APPLICATION_NAME

COMMAND_EXECUTION_UUID = str(uuid.uuid4())


@dataclass(repr=False)
class LoggingContext:
    """Collection of values that can be set during execution and will be sent in every log event

    Example:
    observability.get_logging_context() returns a thread-local context that can be modified
    >>> from karadoc.common.observability import logging_context, LogEvent
    >>> logging_context.get_logging_context().model_id = "s.t1"

    The set values will appear in the LogEvent
    >>> LogEvent(message="Hello!", event_type="test").context.model_id
    's.t1'
    """

    application_name: Optional[str] = None
    "Typical value: Karadoc"
    environment: Optional[str] = None
    "Typical value: PR"
    user_name: Optional[str] = None
    "Typical value: JohnDoe"
    command_uuid: Optional[str] = None
    "Unique identifier of the current running command"
    job_uuid: Optional[str] = None
    "Unique identifier of the current running job"
    thread_uuid: Optional[str] = None
    "Unique identifier of the current thread"
    command: Optional[str] = None
    "Typical value: 'run --tables s.t1 s.t2'"
    model_id: Optional[str] = None
    "Typical value: 's.t1'"
    job_type: Optional[str] = None
    "Typical value: SparkJobBatch, QualityCheck, ..."
    job_vars: Optional[Dict[str, str]] = None
    "Typical value: {'day':'YYYY-MM-DD'}"

    def __repr__(self):
        key_values_str = ", ".join(
            [
                f"{key}={repr(value)}"
                for key, value in self.__dict__.items()
                if value is not None and key not in ["thread_id"]
            ]
        )
        res = f"{self.__class__.__name__}({key_values_str})"
        return res


__MAIN_LOGGING_CONTEXT = LoggingContext()
__THREAD_LOCAL_LOGGING_CONTEXT = threading.local()


def get_logging_context() -> LoggingContext:
    """Get a thread-local LoggingContext that can be modified and will be automatically be passed
    to every LogEvent created.

    Example:
    observability.get_logging_context() returns a thread-local context that can be modified
    >>> from karadoc.common.observability import logging_context, LogEvent
    >>> logging_context.get_logging_context().model_id = "s.t1"

    The set values will appear in the LogEvent
    >>> LogEvent(message="Hello!", event_type="test").context.model_id
    's.t1'
    """
    if threading.current_thread().ident == threading.main_thread().ident:
        if __MAIN_LOGGING_CONTEXT.command_uuid is None:
            __MAIN_LOGGING_CONTEXT.command_uuid = str(uuid.uuid4())
            __MAIN_LOGGING_CONTEXT.environment = conf.get_env()
            __MAIN_LOGGING_CONTEXT.user_name = getpass.getuser()
            __MAIN_LOGGING_CONTEXT.application_name = APPLICATION_NAME
            __MAIN_LOGGING_CONTEXT.thread_id = str(uuid.uuid4())
        return __MAIN_LOGGING_CONTEXT
    if not hasattr(__THREAD_LOCAL_LOGGING_CONTEXT, "value"):
        __THREAD_LOCAL_LOGGING_CONTEXT.value = copy.deepcopy(__MAIN_LOGGING_CONTEXT)
        __THREAD_LOCAL_LOGGING_CONTEXT.value.thread_id = str(uuid.uuid4())
    return __THREAD_LOCAL_LOGGING_CONTEXT.value
