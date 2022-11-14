import traceback
from typing import Generator, List, Optional, Union

from karadoc.common.conf import CONNECTION_GROUP, get_conn_conf_id
from karadoc.common.connector import (
    ConfigurableConnector,
    Connector,
    _load_connector_for_env,
)
from karadoc.common.exceptions import MissingConfigurationException
from karadoc.common.job_core.has_disable import HasDisable
from karadoc.common.validations import (
    ValidationResult,
    ValidationResultTemplate,
    ValidationSeverity,
)
from karadoc.spark.job_core.has_external_inputs import HasExternalInputs
from karadoc.spark.job_core.has_external_outputs import HasExternalOutputs
from karadoc.spark.job_core.has_stream_external_inputs import HasStreamExternalInputs
from karadoc.spark.job_core.has_stream_external_output import HasStreamExternalOutput

ValidationResult_DisabledConnection = ValidationResultTemplate(
    check_type="karadoc.connection.disabled",
    message_template="The connection {conn_conf_id} is disabled",
    default_severity=ValidationSeverity.Error,
)

ValidationResult_MissingConnection = ValidationResultTemplate(
    check_type="karadoc.external_io.missing_connection",
    message_template='{io_type} {io_name} has no "connection" parameter',
    default_severity=ValidationSeverity.Error,
)

ValidationResult_MissingConnectionConfiguration = ValidationResultTemplate(
    check_type="karadoc.external_io.missing_connection_configuration",
    message_template="Could not find configuration {conn_conf_id}",
    default_severity=ValidationSeverity.Error,
)

ValidationResult_MissingConnectionConfigurationDisabledJob = ValidationResultTemplate(
    check_type="karadoc.external_io.missing_connection_configuration_disabled_job",
    message_template="Could not find configuration {conn_conf_id} but the job is disabled",
    default_severity=ValidationSeverity.Warning,
)

ValidationResult_ConnectionLoadError = ValidationResultTemplate(
    check_type="karadoc.connection.loading_error",
    message_template="Could not load connector for {conn_conf_id} :\n {cause}",
    default_severity=ValidationSeverity.Critical,
)


def connection_is_not_disabled(
    job: Union[HasExternalInputs, HasExternalOutputs, HasDisable], conn_conf_id: str, connection: Connector, **_
) -> Generator[ValidationResult, None, None]:
    """Yield an error if the connection is disabled and the job is not"""
    if connection.conf.get("disable") and not job.disable:
        yield ValidationResult_DisabledConnection(conn_conf_id=conn_conf_id)


def _validate_configurable_connection(connection: ConfigurableConnector, conn_conf_id: str):
    for validation_result in connection.validate_params():
        validation_result.message += f" in connection {conn_conf_id}"
        yield validation_result


def load_and_validate_external_io(
    job: HasDisable, io_type: str, io_name: str, io_conf: dict, env: Optional[str], **kwargs
) -> Generator[ValidationResult, None, None]:
    """Yield an error if the connection cannot be loaded"""
    conn_name = io_conf.get(CONNECTION_GROUP)
    conn_conf_id = get_conn_conf_id(conn_name, group=CONNECTION_GROUP, env=env)
    if conn_name is None:
        yield ValidationResult_MissingConnection(io_type=io_type, io_name=io_name)
    else:
        try:
            connection = _load_connector_for_env(conn_name, None, env)
            if isinstance(connection, ConfigurableConnector) and not connection.disable.get():
                yield from _validate_configurable_connection(connection, conn_conf_id)
        except MissingConfigurationException:
            if job.disable:
                yield ValidationResult_MissingConnectionConfigurationDisabledJob(conn_conf_id=conn_conf_id)
            else:
                yield ValidationResult_MissingConnectionConfiguration(conn_conf_id=conn_conf_id)
        except Exception:
            yield ValidationResult_ConnectionLoadError(conn_conf_id=conn_conf_id, cause=traceback.format_exc())
        else:
            yield from connection_is_not_disabled(job=job, conn_conf_id=conn_conf_id, connection=connection, **kwargs)


def __validate_connections_on_env(
    job: Union[HasExternalInputs, HasExternalOutputs, HasStreamExternalInputs, HasStreamExternalOutput, HasDisable],
    env: Optional[str],
) -> Generator[ValidationResult, None, None]:
    if isinstance(job, HasExternalInputs) or isinstance(job, HasStreamExternalInputs):
        for io_name, io_conf in job.external_inputs.items():
            yield from load_and_validate_external_io(
                job=job, io_type="external_input", io_name=io_name, io_conf=io_conf, env=env
            )
    if isinstance(job, HasExternalOutputs):
        for io_name, io_conf in job.external_outputs.items():
            yield from load_and_validate_external_io(
                job=job, io_type="external_output", io_name=io_name, io_conf=io_conf, env=env
            )
    if isinstance(job, HasStreamExternalOutput) and job.external_output is not None:
        yield from load_and_validate_external_io(
            job=job, io_type="external_output", io_name="", io_conf=job.external_output, env=env
        )


def validate_connections(
    job: Union[HasExternalInputs, HasExternalOutputs, HasStreamExternalInputs, HasStreamExternalOutput, HasDisable],
    envs: List[str] = None,
) -> Generator[ValidationResult, None, None]:
    """Performs various checks on the connections used by the given job.
     If a list of environment names is provided, the connections will be validated against each environment.
     If no list is provided, connections will only be validated against the current environment.

    :param job: A action job
    :param envs: A list of execution environments to check (set with ENV_FOR_DYNACONF)
    :return: A list of issues found
    """
    if envs is None:
        envs = [None]
    for env in envs:
        yield from __validate_connections_on_env(job, env)
