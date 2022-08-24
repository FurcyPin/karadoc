from typing import Mapping, Optional

from karadoc.common.observability import LogEvent


class ConsoleEvent(LogEvent):
    """Special type of LogEvent that gets printed on stdout for user-friendly feedback"""

    extra_console_message: Optional[str] = None

    def __init__(
        self,
        message: str,
        event_type: str,
        extra: Optional[Mapping[str, str]] = None,
        extra_console_message: Optional[str] = None,
    ) -> None:
        super().__init__(message, event_type, extra)
        self.extra_console_message = extra_console_message
