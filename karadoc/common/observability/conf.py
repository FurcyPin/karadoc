import logging
import sys
from typing import Optional

from dynaconf.utils.boxing import DynaBox

from karadoc.common.conf.package import get_observability_conf
from karadoc.common.observability import LogEvent
from karadoc.common.observability.logging import setup_logging

LOG = logging.getLogger(__name__)
OBSERVABILITY_CONF_GROUP = "observability"

already_setup = False
observability: Optional[DynaBox] = None


def setup_observability(reset: bool = False, verbose: Optional[bool] = None) -> None:
    """Configure observability on the current process"""
    global already_setup
    if already_setup and not reset:
        return
    observability_conf = get_observability_conf()
    setup_logging(observability_conf, verbose)
    sys.excepthook = __exception_hook
    already_setup = True


def __exception_hook(_exc_type, value, _exc_tb) -> None:
    """Executed on uncaught exceptions"""
    LOG.error(LogEvent("Some uncaught crash occurred", event_type="uncaught_crash"), exc_info=value)
