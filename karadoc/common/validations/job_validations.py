from typing import Generator, Union

from karadoc.common import table_utils
from karadoc.common.job_core.has_inputs import HasInputs
from karadoc.common.job_core.has_keys import HasKeys
from karadoc.common.job_core.has_output import HasOutput
from karadoc.common.job_core.job_base import JobBase
from karadoc.common.validations import (
    ValidationResult,
    ValidationResultTemplate,
    ValidationSeverity,
)

ValidationResult_UnknownInput = ValidationResultTemplate(
    check_type="karadoc.unkown_input",
    message_template="Unknown input table: {table}",
    default_severity=ValidationSeverity.Error,
)

ValidationResult_InvalidOutputPartition = ValidationResultTemplate(
    check_type="karadoc.invalid_output_partition",
    message_template='Output partition "{partition_definition}" is invalid: {cause}',
    default_severity=ValidationSeverity.Error,
)

ValidationResult_SecondaryKeyWithoutPrimaryKey = ValidationResultTemplate(
    check_type="karadoc.secondary_key_without_primary_key",
    message_template="A table should not have secondary keys without a primary key",
    default_severity=ValidationSeverity.Error,
)

ValidationResult_JobLoadError = ValidationResultTemplate(
    check_type="karadoc.job.loading_error",
    message_template="{error_message}:\n{cause}",
    default_severity=ValidationSeverity.Critical,
)


def validate_inputs(job: Union[HasInputs, JobBase], **_) -> Generator[ValidationResult, None, None]:
    """Check that inputs correspond to inputs managed by karadoc"""
    for input_table in job.list_input_tables():
        if not table_utils.table_exists(input_table):
            yield ValidationResult_UnknownInput(table=input_table)


def validate_output_partition(job: HasOutput, **_) -> Generator[ValidationResult, None, None]:
    """Check format of output partitions"""
    # TODO #90277 : refactor format of output partitions: dynamic partitions could be dicts
    has_dynamic_partition = False
    for partition_definition in job.output_partition:
        if type(partition_definition) == str:
            if not partition_definition:
                yield ValidationResult_InvalidOutputPartition(
                    partition_definition=partition_definition, cause="empty value"
                )
            has_dynamic_partition = True
        elif type(partition_definition) == tuple:
            if len(partition_definition) != 2:
                yield ValidationResult_InvalidOutputPartition(
                    partition_definition=partition_definition, cause="tuple is not valid (2 elements needed)"
                )
            elif type(partition_definition[0]) != str:
                yield ValidationResult_InvalidOutputPartition(
                    partition_definition=partition_definition, cause="tuple is not valid (first element need to be str)"
                )
            elif not partition_definition[0] or not partition_definition[1]:
                yield ValidationResult_InvalidOutputPartition(
                    partition_definition=partition_definition, cause="empty value"
                )
            elif has_dynamic_partition:
                yield ValidationResult_InvalidOutputPartition(
                    partition_definition=partition_definition,
                    cause="static partition defined after a dynamic partition",
                )
        else:
            yield ValidationResult_InvalidOutputPartition(
                partition_definition=partition_definition, cause="invalid type"
            )


def validate_keys(job: HasKeys, **_) -> Generator[ValidationResult, None, None]:
    """Checks conditions on keys"""
    if len(job.secondary_keys) > 0 and job.primary_key is None:
        yield ValidationResult_SecondaryKeyWithoutPrimaryKey()
