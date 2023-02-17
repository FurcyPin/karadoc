from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING, Optional

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
from karadoc.common.exceptions import JobDisabledException
from karadoc.common.model import variables
from karadoc.common.validations import fail_if_results
from karadoc.common.validations.connection_validations import validate_connections
from karadoc.common.validations.job_validations import validate_inputs, validate_output_partition
from karadoc.spark.batch.spark_batch_job import SparkBatchJob
from karadoc.spark.job_core.has_spark import _partition_to_path

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from spark_frame.data_diff.diff_results import DiffResult


def _get_join_cols(args, job):
    if args.nonreg_join_cols:
        join_cols = list(args.nonreg_join_cols.split(","))
    elif job.standardized_primary_key is not None:
        join_cols = list(job.standardized_primary_key)
    else:
        join_cols = None
    return join_cols


def _check_nonreg(job: SparkBatchJob, df: "DataFrame", args):
    """Compare the output of a Populate with the currently existing one."""
    from pyspark.sql import DataFrame
    from spark_frame.data_diff.dataframe_comparator import DataframeComparator
    from spark_frame.data_diff.diff_format_options import DiffFormatOptions

    if conf.is_dev_env():
        job.spark.conf.set("spark.sql.shuffle.partitions", "200")

    previous_df = get_previous_df(job=job, compare_env=args.compare_env)

    if df is None or not isinstance(df, DataFrame):
        print("ERROR: The job did not return any DataFrame.")
        return ReturnCode.Error

    if len(df.columns) == 0:
        print("ERROR: The job returned a DataFrame with zero columns.")
        return ReturnCode.Error

    join_cols = _get_join_cols(args, job)

    if args.nonreg_filter:
        df = df.filter(args.nonreg_filter)
        previous_df = previous_df.filter(args.nonreg_filter)

    ignore_cols = args.ignore_cols
    if args.ignore_cols is not None:
        previous_df = previous_df.drop(*ignore_cols)
        df = df.drop(*ignore_cols)

    df.localCheckpoint()
    nonreg = DataframeComparator(
        DiffFormatOptions(nb_diffed_rows=args.nonreg_nb_rows, left_df_alias="before", right_df_alias="after")
    )
    nonreg_result = nonreg.compare_df(previous_df, df, join_cols=join_cols)
    nonreg_result.display(show_examples=args.show_examples)
    inspect_nonreg_results(nonreg_result)
    if nonreg_result.is_ok:
        return ReturnCode.Success
    else:
        return ReturnCode.Error


def get_previous_df(job, compare_env):
    if compare_env is not None:
        partition_path = _partition_to_path(job.output_partition)
        output = f"{job.output}{partition_path}"
        previous_df_hdfs_location = job.hdfs_input(output, remote_env_name=compare_env)
    else:
        previous_df_hdfs_location = job.hdfs_output()
    previous_df = job._read_path(previous_df_hdfs_location, job.output_format)
    return previous_df


def inspect_nonreg_results(df: "DiffResult"):
    """Function used for unit tests. Mock this function to be able to inspect the produced DataFrame
    during the execution of the command."""
    pass


def _run_table(job: "SparkBatchJob", args) -> Optional["DataFrame"]:
    """Computes the Spark DataFrame build by a SparkBatchJob with the specified args.

    :param job:
    :param args:
    :return:
    """
    from karadoc.spark.batch.exec import print_job_properties

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
        df = job.run()
    elif not conf.is_dev_env():
        init_job(job=job, raw_args=args.raw_args, spark_conf=args.spark_conf)
    return df


class NonregCommand(Command):
    description = "spot differences for the specified tables"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        parser.formatter_class = LineBreakHelpFormatter

        TablesOption.add_arguments(parser)
        DryOption.add_arguments(parser)
        ReadFromOption.add_arguments(parser)
        VarsOption.add_arguments(parser)
        SparkConfOption.add_arguments(parser)
        parser.add_argument(
            "--filter",
            dest="nonreg_filter",
            type=str,
            help="Filter to apply to each DataFrame before comparing them."
            " (works only with the --nonreg option)"
            """\nExample: --nonreg-filter "day > '2020-07-25'" """,
        )
        parser.add_argument(
            "--nb-rows",
            dest="nonreg_nb_rows",
            type=int,
            default=10,
            help="Number of differing rows to show when performing a non-reg (works only with the --nonreg option)",
        )
        parser.add_argument(
            "--join-cols",
            dest="nonreg_join_cols",
            type=str,
            help="Comma-separated list of columns names to use to perform the nonreg diff"
            " (works only with the --nonreg option)",
        )
        parser.add_argument(
            "--compare-with",
            dest="compare_env",
            metavar="ENV",
            help="Compare with data from a remote warehouse in env ENV instead of the local warehouse",
        )
        parser.add_argument(
            "--ignore-cols", dest="ignore_cols", type=str, nargs="*", help="Columns specified will be ignored"
        )
        parser.add_argument(
            "--show-examples",
            dest="show_examples",
            default=False,
            action="store_true",
            help="If set, print for each column examples of full rows where this column changes",
        )

    @staticmethod
    def do_command(args: Namespace) -> ReturnCode:
        from karadoc.spark.batch.exec import load_runnable_populate

        return_code = ReturnCode.Success
        vars_list = variables.expand_vars(args.vars)
        for table in args.tables:
            for vars in vars_list:
                print("\nStarting table %s" % table)
                job: SparkBatchJob = load_runnable_populate(table, vars)
                df = _run_table(job, args)
                if df is not None:
                    return_code = _check_nonreg(job, df, args)
        return return_code
