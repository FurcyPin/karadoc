from typing import Dict, Generator, List, Optional, Tuple

from karadoc.common import conf
from karadoc.common.conf import (
    CONNECTION_GROUP,
    get_conn_conf_id,
    list_connection_names,
)
from karadoc.common.job_core.has_disable import HasDisable
from karadoc.common.job_core.job_base import JobBase
from karadoc.common.job_core.load import load_non_runnable_action_file
from karadoc.common.model import file_index
from karadoc.common.validations import (
    ValidationResult,
    ValidationResultTemplate,
    ValidationSeverity,
)
from karadoc.spark.batch.spark_batch_job import SparkBatchJob
from karadoc.spark.job_core.has_external_inputs import HasExternalInputs
from karadoc.spark.job_core.has_external_outputs import HasExternalOutputs
from karadoc.spark.job_core.has_stream_external_inputs import HasStreamExternalInputs
from karadoc.spark.job_core.has_stream_external_output import HasStreamExternalOutput
from karadoc.spark.quality.quality_check_job import QualityCheckJob
from karadoc.spark.stream.spark_stream_job import SparkStreamJob

ValidationResult_UnusedConnection = ValidationResultTemplate(
    check_type="karadoc.connection.unused",
    message_template="The connection {conn_conf_id} is not used by any job",
    default_severity=ValidationSeverity.Warning,
)

ValidationResult_ConnectionCouldBeDisabled = ValidationResultTemplate(
    check_type="karadoc.connection.could_be_disabled",
    message_template="The connection {conn_conf_id} is only used by disable jobs and could be disabled",
    default_severity=ValidationSeverity.Warning,
)


def __list_job_io(job: JobBase) -> Generator[dict, None, None]:
    if isinstance(job, HasExternalInputs) or isinstance(job, HasStreamExternalInputs):
        yield from job.external_inputs.values()
    if isinstance(job, HasExternalOutputs):
        yield from job.external_outputs.values()
    if isinstance(job, HasStreamExternalOutput):
        yield job.external_output


def __list_connections_used_by_job(job: JobBase):
    if isinstance(job, HasDisable):
        job_disabled = job.disable
    else:
        job_disabled = False
    for source_details in __list_job_io(job):
        conn = source_details.get(CONNECTION_GROUP)
        if conn is not None:
            yield conn, job_disabled


def __list_used_connections() -> Generator[Tuple[str, bool], None, None]:
    """Return the list of all connections used by action files along with a boolean indicating if the job is disabled"""
    for file_type in [SparkBatchJob, SparkStreamJob, QualityCheckJob]:
        for schema, table, action_file in file_index.list_action_files(file_type):
            full_table_name = schema + "." + table
            job = load_non_runnable_action_file(full_table_name, file_type)
            yield from __list_connections_used_by_job(job)


def _list_used_connections() -> Dict[str, bool]:
    """Return the a dict of all connections used by action files along with a boolean indicating
    if all the job using it are disabled"""
    used_connections = {}
    for conn, job_disabled in __list_used_connections():
        if conn not in used_connections:
            used_connections[conn] = True
        used_connections[conn] &= job_disabled
    return used_connections


def _is_conn_disabled(conn_name, env) -> bool:
    conn_disabled = conf.get_connection_conf(conn_name, env).get("disable")
    if conn_disabled is None:
        conn_disabled = False
    return conn_disabled


def validate_unused_connections(envs: Optional[List[str]]) -> Generator[ValidationResult, None, None]:
    """Return the list of all connections defined in the settings for all the specified envs
    and not used by any action file"""
    used_connections = _list_used_connections()
    if envs is None:
        envs = [None]
    for env in envs:
        for conn_name in list_connection_names(env):
            conn_conf_id = get_conn_conf_id(conn_name, group=CONNECTION_GROUP, env=env)
            if conn_name not in used_connections:
                yield ValidationResult_UnusedConnection(conn_conf_id=conn_conf_id)
            else:
                all_job_using_conn_are_disabled = used_connections[conn_name]
                if not _is_conn_disabled(conn_name, env) and all_job_using_conn_are_disabled:
                    yield ValidationResult_ConnectionCouldBeDisabled(conn_conf_id=conn_conf_id)
