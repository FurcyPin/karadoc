import traceback
from argparse import ArgumentParser
from typing import Callable, Iterator, List, Optional, Type

from karadoc.common.analyze.analyze_job import AnalyzeJob
from karadoc.common.commands.command import Command
from karadoc.common.commands.return_code import ReturnCode
from karadoc.common.exceptions import ActionFileLoadingError
from karadoc.common.job_core.job_base import JobBase
from karadoc.common.job_core.load import load_non_runnable_action_file
from karadoc.common.model import file_index
from karadoc.common.quality.quality_check_job import QualityCheckJob
from karadoc.common.run.spark_batch_job import SparkBatchJob
from karadoc.common.stream.spark_stream_job import SparkStreamJob
from karadoc.common.utils.assert_utils import assert_true
from karadoc.common.validations import ValidationResult, ValidationSeverity
from karadoc.common.validations.connection_validations import validate_connections
from karadoc.common.validations.job_validations import (
    ValidationResult_JobLoadError,
    validate_inputs,
    validate_keys,
    validate_output_partition,
)
from karadoc.common.validations.quality_check_validations import (
    validate_alert_duplicate_names,
    validate_metric_duplicate_names,
)
from karadoc.common.validations.unused_connection_validations import (
    validate_unused_connections,
)

TOTAL = "total"
OK = "ok"
FATAL = "fatal"
NON_FATAL = "non_fatal"


def inspect_results(results: List[ValidationResult]):
    """Function used for unit tests. Mock this function to be able to inspect the produced results
    during the execution of the command."""
    pass


def __get_non_fatal_count_str(non_fatal_count, severity_threshold):
    non_fatal_count_str = ""
    if non_fatal_count > 0:
        plural = "" if non_fatal_count <= 1 else "s or lower"
        non_fatal_count_str = f" {non_fatal_count} {severity_threshold.decrease().repr_item}{plural}."
    return non_fatal_count_str


def __get_fatal_count_str(fatal_count, severity_threshold):
    fatal_count_str = ""
    if fatal_count > 0:
        plural = "" if fatal_count <= 1 else "s or higher"
        fatal_count_str = f" {fatal_count} {severity_threshold.repr_item}{plural}."
    return fatal_count_str


def _validate_action_file(
    full_table_name: str, job_type: Type, validations: List[Callable], envs: Optional[List[str]]
) -> Iterator[ValidationResult]:
    """Load the action file for the specified table and job type, and apply validations on it.

    :param full_table_name: The full name of the table to validate
    :param job_type: The type of job to validate
    :param validations: The list of validation functions to apply
    :param envs: An optional list of environment against which the validations must be run
    :return:
    """
    try:
        job = load_non_runnable_action_file(full_table_name, job_type)
    except ActionFileLoadingError as e:
        yield ValidationResult_JobLoadError(error_message=e.args[0], cause=traceback.format_exc())
    else:
        for validation in validations:
            yield from validation(job, envs=envs)


def _print_results(results):
    for res in results:
        print(res.color_str() + "\n")


def _validate_action_files(
    job_type: Type, validations: List[Callable], envs: Optional[List[str]], severity_threshold=ValidationSeverity.Error
):
    assert_true(issubclass(job_type, JobBase))

    counters = {TOTAL: 0, OK: 0, FATAL: 0, NON_FATAL: 0}
    for schema, table, action_file in file_index.list_action_files(job_type):
        full_table_name = schema + "." + table
        counters[TOTAL] += 1
        results = list(_validate_action_file(full_table_name, job_type, validations, envs))

        nb_non_fatal_results = len([r for r in results if r.severity < severity_threshold])
        nb_fatal_results = len([r for r in results if r.severity >= severity_threshold])

        if len(results) > 0:
            inspect_results(results)
            print(full_table_name + " at " + action_file + " :")
            _print_results(results)

        _update_counters(counters, nb_fatal_results, nb_non_fatal_results)

    fatal_count_str = __get_fatal_count_str(counters[FATAL], severity_threshold)
    non_fatal_count_str = __get_non_fatal_count_str(counters[NON_FATAL], severity_threshold)
    print(
        f"{counters[OK]} / {counters[TOTAL]} {job_type.get_action_file_name()} "
        f"validated.{fatal_count_str}{non_fatal_count_str}"
    )

    if counters[FATAL] > 0:
        return ReturnCode.Error
    else:
        return ReturnCode.Success


def _update_counters(counters, nb_fatal_results, nb_non_fatal_results):
    if nb_fatal_results == 0 and nb_non_fatal_results == 0:
        counters[OK] += 1
    else:
        if nb_fatal_results == 0:
            counters[NON_FATAL] += 1
        else:
            counters[FATAL] += 1


def _validate_unused_connections(envs: Optional[List[str]], severity_threshold=ValidationSeverity.Error):
    results = list(validate_unused_connections(envs))
    if len(results) > 0:
        inspect_results(results)
    for res in results:
        print(res.color_str())
    non_fatal_count = len([r for r in results if r.severity < severity_threshold])
    fatal_count = len([r for r in results if r.severity >= severity_threshold])
    fatal_count_str = __get_fatal_count_str(fatal_count, severity_threshold)
    non_fatal_count_str = __get_non_fatal_count_str(non_fatal_count, severity_threshold)
    print(f"{len(results)} unused connections found.{fatal_count_str}{non_fatal_count_str}")
    if fatal_count > 0:
        return ReturnCode.Error
    else:
        return ReturnCode.Success


class ValidateCommand(Command):
    description = "apply validation checks on all tables and connections referenced in the project"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--coverage",
            dest="coverage",
            default=False,
            action="store_true",
            help="Activate this option to generate code coverage report on the files that were tested.",
        )
        parser.add_argument(
            "--envs",
            dest="envs",
            metavar="ENV",
            type=str,
            default=None,
            nargs="+",
            help="Validate connections against each one of the specified environments. By default, connections are only"
            "validated against the current environment.",
        )

    @staticmethod
    def do_command(args) -> ReturnCode:
        return_code = ReturnCode.Success
        cov = None
        if args.coverage:
            import coverage

            cov = coverage.Coverage()
            cov.start()

        return_code |= _validate_action_files(
            job_type=SparkBatchJob,
            validations=[
                validate_inputs,
                validate_output_partition,
                validate_connections,
                validate_keys,
            ],
            envs=args.envs,
        )
        return_code |= _validate_action_files(
            job_type=SparkStreamJob,
            validations=[
                validate_connections,
            ],
            envs=args.envs,
        )
        return_code |= _validate_action_files(
            job_type=QualityCheckJob,
            validations=[
                validate_alert_duplicate_names,
                validate_metric_duplicate_names,
                validate_connections,
            ],
            envs=args.envs,
        )
        return_code |= _validate_action_files(job_type=AnalyzeJob, validations=[], envs=args.envs)

        try:
            return_code |= _validate_unused_connections(envs=args.envs)
        except ActionFileLoadingError:
            pass

        if cov is not None:
            cov.stop()
            cov.save()

        return return_code
