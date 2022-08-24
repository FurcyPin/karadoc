from argparse import ArgumentParser
from typing import List

from termcolor import colored

from karadoc.common.commands.command import Command
from karadoc.common.commands.return_code import ReturnCode
from karadoc.common.observability.conf import setup_observability
from karadoc.common.validations import ValidationResult, ValidationSeverity


def count_results_with_severity(validation_results: List[ValidationResult], severity: ValidationSeverity) -> int:
    return len([res for res in validation_results if res.severity == severity])


def inspect_results(results: List[ValidationResult]):
    """Function used for unit tests. Mock this function to be able to inspect the produced results
    during the execution of the command."""
    pass


class ValidateAirflowCommand(Command):
    description = "perform various checks on the airflow dags"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--coverage",
            dest="coverage",
            default=False,
            action="store_true",
            help="Activate this option to generate code coverage report on the files that where tested.",
        )
        parser.add_argument(
            "--max-warnings",
            dest="max_warnings",
            metavar="N",
            type=int,
            help="Returns an error code if more than N warning are found.",
        )

    @staticmethod
    def do_command(args) -> ReturnCode:
        return_code = ReturnCode.Success
        cov = None
        if args.coverage:
            import coverage

            cov = coverage.Coverage()
            cov.start()

        from karadoc.airflow.dag_graph import build_dag_bag
        from karadoc.airflow.validate import validate_dag_bag
        from karadoc.common.model import table_index

        table_index = table_index.build_table_index()
        dag_bag = build_dag_bag()

        # We force the reset of the logging conf after loading the DAG bag because Airflow overrode our configuration
        setup_observability(reset=True)

        validation_results = validate_dag_bag(dag_bag, table_index)

        if len(validation_results) > 0:
            inspect_results(validation_results)

        for result in validation_results:
            print(result)

        num_errors = count_results_with_severity(validation_results, ValidationSeverity.Error)
        if num_errors > 0:
            print(colored(f"Found {num_errors} errors", "red"))
            return_code = ReturnCode.Error

        num_warnings = count_results_with_severity(validation_results, ValidationSeverity.Warning)
        if args.max_warnings is not None and num_warnings > args.max_warnings:
            print(
                colored(
                    f"Found {num_warnings} warnings, which is greater than option --max-warnings {args.max_warnings}",
                    "red",
                )
            )
            return_code = ReturnCode.Error

        if cov is not None:
            cov.stop()
            cov.save()

        return return_code
