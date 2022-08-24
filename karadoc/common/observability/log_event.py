import copy
from dataclasses import dataclass
from typing import Mapping, Optional

from karadoc.common.observability.logging_context import (
    LoggingContext,
    get_logging_context,
)


@dataclass(init=False)
class LogEvent:
    """Structured logging data"""

    message: str
    """Human-readable message"""
    event_type: str
    """Event type, useful for grouping and filtering"""
    extra: Optional[Mapping[str, str]]
    """Additional information specific to this event type"""
    context: LoggingContext
    """Static thread-local context information modified by the system"""

    def __init__(self, message: str, event_type: str, extra: Optional[Mapping[str, str]] = None):
        self.message = message
        self.event_type = event_type
        self.extra = extra
        self.context = copy.deepcopy(get_logging_context())
