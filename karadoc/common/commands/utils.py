import logging
import uuid
from typing import Callable, Dict, Type

from karadoc.common.commands.return_code import ReturnCode
from karadoc.common.exceptions import LoggedException
from karadoc.common.observability import get_logging_context
from karadoc.common.observability.console_event import ConsoleEvent
from karadoc.common.utils.stopwatch import Stopwatch

LOG = logging.getLogger(__name__)


def run_job_with_logging(func: Callable, job_type: Type, model_id: str, job_vars: Dict[str, str], **kwargs):
    logging_context = get_logging_context()
    logging_context.model_id = model_id
    logging_context.job_vars = job_vars
    logging_context.job_type = str(job_type.__name__)
    logging_context.job_uuid = str(uuid.uuid4())
    LOG.user_output(ConsoleEvent(message=f"Starting job for model: {model_id}", event_type="job_start"))
    stopwatch = Stopwatch()
    try:
        func(**{"model_id": model_id, "job_vars": job_vars, **kwargs})
        extra: Dict[str, str] = {"status": "success", "duration_in_seconds": stopwatch.duration_in_seconds}
        LOG.user_output(
            ConsoleEvent(
                message=f"Job ended for model: {model_id}",
                event_type="job_end",
                extra=extra,
                extra_console_message=f"({stopwatch.duration_str})",
            )
        )
        return ReturnCode.Success
    except Exception as err:
        extra: Dict[str, str] = {"status": "failed", "duration_in_seconds": stopwatch.duration_in_seconds}
        LOG.error(
            ConsoleEvent(
                message=f"Job failed for model: {model_id}",
                event_type="job_end",
                extra=extra,
                extra_console_message=f"({stopwatch.duration_str})",
            ),
            exc_info=err,
        )
        raise LoggedException() from err
