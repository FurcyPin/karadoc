import sys
from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING

from karadoc.common import conf
from karadoc.common.commands.command import Command
from karadoc.common.commands.options.dry_option import DryOption
from karadoc.common.commands.options.spark_conf_option import SparkConfOption
from karadoc.common.commands.options.tables_option import ModelsOption
from karadoc.common.commands.options.vars_option import VarsOption
from karadoc.common.commands.return_code import ReturnCode
from karadoc.common.commands.utils import run_job_with_logging
from karadoc.common.exceptions import JobDisabledException
from karadoc.common.model import variables
from karadoc.common.validations import fail_if_results
from karadoc.common.validations.connection_validations import validate_connections
from karadoc.common.validations.job_validations import validate_inputs, validate_output_partition
from karadoc.spark.stream.spark_stream_job import SparkStreamJob

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def inspect_df(df: "DataFrame"):
    """Function used for unit tests. Mock this function to be able to inspect the produced DataFrame
    during the execution of the command."""
    pass


def inspect_job(job: SparkStreamJob):
    """Function used for unit tests. Mock this function to be able to inspect the SparkStreamJob object
    during the execution of the command."""
    pass


def _run_job(args: Namespace, job: SparkStreamJob, **kwargs):
    from karadoc.spark.stream.exec import print_job_properties, write_stream_output

    print_job_properties(job)
    fail_if_results(validate_connections(job))
    fail_if_results(validate_inputs(job))
    fail_if_results(validate_output_partition(job))
    if job.disable:
        if not args.dry:
            raise JobDisabledException()
        else:
            print("WARN: The job has been disabled, it should not be launched")
    if args.batch_interval is not None and args.streaming_mode != "microbatch":
        print("ERROR: batch_interval is only specified when streaming mode is microbatch")
        sys.exit(1)
    if len(args.models) > 1:
        print("ERROR: multiple models are not supported for the moment in streaming mode")
        sys.exit(1)
    if not args.dry:
        job.init(app_name=args.raw_args, spark_conf=args.spark_conf)
        df = job.stream()
        inspect_df(df)
        write_stream_output(job, df, args.no_export, args.streaming_mode, args.batch_interval)
    elif not conf.is_dev_env():
        job.init()
    inspect_job(job)


class StreamCommand(Command):
    description = "run STREAM.py scripts for the specified tables"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        ModelsOption.add_arguments(parser)
        DryOption.add_arguments(parser)
        VarsOption.add_arguments(parser)
        SparkConfOption.add_arguments(parser)

        parser.add_argument(
            "--no-export", dest="no_export", default=False, action="store_true", help="Disable exporting data"
        )
        parser.add_argument(
            "--streaming-mode",
            dest="streaming_mode",
            default="microbatch",
            type=str,
            choices=["once", "microbatch"],
            help="(Default: microbatch) Define the timing of streaming data processing, whether the query is going "
            "to be executed as micro-batch query with a fixed batch interval or as a one batch query.",
        )
        parser.add_argument(
            "--batch-interval",
            dest="batch_interval",
            type=str,
            help="The query will be executed with micro-batches mode, "
            "where micro-batches will be kicked off at the user-specified intervals"
            "batch-interval time interval is a string, e.g. '5 seconds', '1 minute'.",
        )

    @staticmethod
    def do_command(args: Namespace):
        from karadoc.spark.stream.exec import load_runnable_stream_file

        return_code = ReturnCode.Success
        vars_list = variables.expand_vars(args.vars)
        jobs = [
            (table, job_vars, load_runnable_stream_file(table, job_vars))
            for table in args.models
            for job_vars in vars_list
        ]
        for model_id, job_vars, job in jobs:
            return_code &= run_job_with_logging(
                func=_run_job, job_type=SparkStreamJob, model_id=model_id, job_vars=job_vars, args=args, job=job
            )
        return return_code
