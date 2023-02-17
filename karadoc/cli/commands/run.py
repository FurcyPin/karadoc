from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Dict, Optional

from karadoc.common import conf
from karadoc.common.commands.command import Command
from karadoc.common.commands.help import LineBreakHelpFormatter
from karadoc.common.commands.options.dry_option import DryOption
from karadoc.common.commands.options.read_from_option import ReadFromOption
from karadoc.common.commands.options.spark_conf_option import SparkConfOption
from karadoc.common.commands.options.tables_option import TablesOption
from karadoc.common.commands.options.vars_option import VarsOption
from karadoc.common.commands.return_code import ReturnCode
from karadoc.common.commands.spark import init_job
from karadoc.common.commands.utils import run_job_with_logging
from karadoc.common.exceptions import JobDisabledException
from karadoc.common.model import variables
from karadoc.common.validations import fail_if_results
from karadoc.common.validations.connection_validations import validate_connections
from karadoc.common.validations.job_validations import validate_inputs, validate_output_partition
from karadoc.spark.batch.exec import load_runnable_populate, print_job_properties, write_output
from karadoc.spark.batch.spark_batch_job import SparkBatchJob

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def inspect_df(df: "DataFrame"):
    """Function used for unit tests. Mock this function to be able to inspect the produced DataFrame
    during the execution of the command."""
    pass


def inspect_job(job: SparkBatchJob):
    """Function used for unit tests. Mock this function to be able to inspect the SparkBatchJob object
    during the execution of the command."""
    pass


def _run_table(job: SparkBatchJob, args) -> Optional["DataFrame"]:
    """Computes the Spark DataFrame build by a SparkBatchJob with the specified args.

    :param job:
    :param args:
    :return:
    """

    df: Optional["DataFrame"] = None
    if args.remote_env is not None:
        job._configure_remote_input(args.remote_env)
    print_job_properties(job)
    fail_if_results(validate_inputs(job))
    fail_if_results(validate_output_partition(job))
    fail_if_results(validate_connections(job))
    if job.disable:
        if not args.dry:
            raise JobDisabledException()
        else:
            print("WARN: The job has been disabled, it should not be launched")
    if not args.dry:
        init_job(job=job, raw_args=args.raw_args, spark_conf=args.spark_conf)
        if args.limit_inputs is not None:
            job._limit_inputs = args.limit_inputs
        if args.limit_external_inputs is not None:
            job._limit_external_inputs = args.limit_external_inputs
        df = job.run()
        if args.limit_output is not None and df is not None:
            df = df.limit(args.limit_output)
    elif not conf.is_dev_env():
        init_job(job=job, raw_args=args.raw_args, spark_conf=args.spark_conf)
    return df


def _run_model(args: Namespace, model_id: str, job_vars: Dict[str, str], **kwargs):
    job: SparkBatchJob = load_runnable_populate(model_id, job_vars)
    df = _run_table(job, args)
    inspect_df(df)
    if df is not None:
        write_output(job, df, no_write=args.no_write, no_export=args.no_export)
    inspect_job(job)


class RunCommand(Command):
    description = "run POPULATE.py scripts for the specified tables"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        parser.formatter_class = LineBreakHelpFormatter

        TablesOption.add_arguments(parser)
        DryOption.add_arguments(parser)
        ReadFromOption.add_arguments(parser)
        VarsOption.add_arguments(parser)
        SparkConfOption.add_arguments(parser)

        parser.add_argument(
            "--limit-output",
            dest="limit_output",
            metavar="N",
            type=int,
            help="Only take the first N rows of the DataFrame returned by the run() method. Useful for testing.",
        )
        parser.add_argument(
            "--limit-inputs",
            dest="limit_inputs",
            metavar="N",
            type=int,
            help="Only take the first N rows of the DataFrame of any input loaded. Useful for testing.",
        )
        parser.add_argument(
            "--limit-external-inputs",
            dest="limit_external_inputs",
            metavar="N",
            type=int,
            help="Only take the first N rows of the DataFrame of any external input loaded. Useful for testing.",
        )
        parser.add_argument(
            "--no-export",
            dest="no_export",
            default=False,
            action="store_true",
            help="Disable writing on external outputs. Useful for testing",
        )
        parser.add_argument(
            "--no-write",
            dest="no_write",
            default=False,
            action="store_true",
            help="Disable writing any output or external output. Useful for testing",
        )
        parser.add_argument(
            "--validate",
            dest="validate",
            default=False,
            action="store_true",
            help="Activate validation mode. This is equivalent to setting all the following options:"
            "\n  --limit-inputs 0"
            "\n  --limit-output 0"
            "\n  --limit-external-inputs 0"
            "\n  --no-write"
            "\n  --no-export"
            "\nWARNING: Please make sure the jobs being validated have no side-effects.",
        )

    @staticmethod
    def do_command(args: Namespace) -> ReturnCode:
        return_code = ReturnCode.Success
        vars_list = variables.expand_vars(args.vars)
        if args.validate:
            args.limit_output = 0
            args.limit_inputs = 0
            args.limit_external_inputs = 0
            args.no_write = True
            args.no_export = True
        for table in args.tables:
            for job_vars in vars_list:
                return_code &= run_job_with_logging(
                    func=_run_model, job_type=SparkBatchJob, args=args, model_id=table, job_vars=job_vars
                )

        return return_code
