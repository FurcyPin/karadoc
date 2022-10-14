from argparse import ArgumentParser, Namespace
from typing import Dict

from karadoc.common import conf
from karadoc.common.commands.command import Command
from karadoc.common.commands.options.dry_option import DryOption
from karadoc.common.commands.options.read_from_option import ReadFromOption
from karadoc.common.commands.options.tables_option import TablesOption
from karadoc.common.commands.options.vars_option import VarsOption
from karadoc.common.commands.return_code import ReturnCode
from karadoc.common.commands.utils import run_job_with_logging
from karadoc.common.exceptions import JobDisabledException
from karadoc.common.model import variables
from karadoc.common.quality.quality_check_job import QualityCheckJob
from karadoc.common.validations import fail_if_results
from karadoc.common.validations.job_validations import validate_inputs


def __execute_quality_checks(table: str, job: QualityCheckJob, args, vars: Dict[str, str]):
    from karadoc.common.quality.exec import execute_alert, execute_metric

    job.before_all()
    for alert in job.alerts:
        job.before_each()
        if args.alerts is None or alert.name in args.alerts:
            execute_alert(table, alert, vars, args.output_alert_table)
        job.after_each()
    for metric in job.metrics:
        job.before_each()
        if args.metrics is None or metric.name in args.metrics:
            execute_metric(table, metric, vars, args.output_metric_table)
        job.after_each()
    job.after_all()


def _run_quality_check_for_model(args: Namespace, model_id: str, job_vars: Dict[str, str], **kwargs):
    from karadoc.common.commands.spark import init_job
    from karadoc.common.quality.exec import load_runnable_quality_check

    job = load_runnable_quality_check(model_id, job_vars)
    fail_if_results(validate_inputs(job))
    if job.disable:
        if not args.dry:
            raise JobDisabledException()
        else:
            print("WARN: The job has been disabled, It should not be launched")
    if not args.dry:
        init_job(job=job, raw_args=args.raw_args)
        if args.remote_env is not None:
            job._configure_remote_input(args.remote_env)
        __execute_quality_checks(model_id, job, args, job_vars)
    elif args.dry and not conf.is_dev_env():
        init_job(job=job, raw_args=args.raw_args)


class QualityCheckCommand(Command):
    description = "run QUALITY_CHECK.py scripts for the specified tables"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        TablesOption.add_arguments(parser)
        DryOption.add_arguments(parser)
        ReadFromOption.add_arguments(parser)
        VarsOption.add_arguments(parser)

        parser.add_argument(
            "--output-alert-table",
            dest="output_alert_table",
            default=None,
            required=False,
            type=str,
            metavar="TABLE",
            help="Write the generated alert DataFrames to the external_outputs of the specified table."
            "This table must have a valid POPULATE.py file, with a run() method that will be ignored.",
        )
        parser.add_argument(
            "--output-metric",
            dest="output_metric_table",
            default=None,
            required=False,
            type=str,
            metavar="TABLE",
            help="Write the generated metric DataFrames to the external_outputs of the specified table. "
            "This table must have a valid POPULATE.py file, with a run() method that will be ignored.",
        )
        parser.add_argument(
            "--alerts",
            dest="alerts",
            default=None,
            required=False,
            nargs="+",
            type=str,
            metavar="ALERTS",
            help="Run only the alerts with the specified names",
        )
        parser.add_argument(
            "--metrics",
            dest="metrics",
            default=None,
            required=False,
            nargs="+",
            type=str,
            metavar="METRICS",
            help="Run only the metrics with the specified names",
        )

    @staticmethod
    def do_command(args) -> ReturnCode:
        return_code = ReturnCode.Success

        vars_list = variables.expand_vars(args.vars)

        for model_id in args.tables:
            for job_vars in vars_list:
                return_code &= run_job_with_logging(
                    func=_run_quality_check_for_model,
                    job_type=QualityCheckJob,
                    model_id=model_id,
                    job_vars=job_vars,
                    args=args,
                )

        return return_code
