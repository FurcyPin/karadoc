from argparse import ArgumentParser, Namespace

from karadoc.common import conf
from karadoc.common.analyze import draw_timeline_analysis, timeline_analysis
from karadoc.common.analyze.analyze_job import AnalyzeJob
from karadoc.common.commands.command import Command
from karadoc.common.commands.options.dry_option import DryOption
from karadoc.common.commands.options.read_from_option import ReadFromOption
from karadoc.common.commands.options.spark_conf_option import SparkConfOption
from karadoc.common.commands.options.tables_option import TablesOption
from karadoc.common.commands.return_code import ReturnCode
from karadoc.common.commands.spark import init_job
from karadoc.common.job_core.load import load_runnable_action_file
from karadoc.common.table_utils import get_data_location
from karadoc.common.utils.assert_utils import assert_true
from karadoc.common.validations import fail_if_results
from karadoc.common.validations.job_validations import validate_inputs


def _print_job_properties(job: AnalyzeJob):
    if job.inputs:
        print("inputs : %s" % job.inputs)


def _check_job(job: AnalyzeJob):
    assert_true(job.reference_time_col is not None, "reference_time_col is not defined")
    assert_true(job.cohorts is not None, "cohort is not defined")
    assert_true(job.nb_buckets is not None, "nb_buckets is not defined")
    assert_true(job.analyze is not None, "job.analyze is not defined")


class AnalyzeTimelineCommand(Command):
    description = "run timeline analysis scripts for the specified tables"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        TablesOption.add_arguments(parser)
        DryOption.add_arguments(parser)
        ReadFromOption.add_arguments(parser)
        SparkConfOption.add_arguments(parser)

    @staticmethod
    def do_command(args: Namespace) -> ReturnCode:
        for table in args.tables:
            print("starting analyzing table %s" % table)
            data_location = get_data_location(table)
            print(data_location)
            job = load_runnable_action_file(table, AnalyzeJob, {})
            _print_job_properties(job)
            fail_if_results(validate_inputs(job))
            _check_job(job)
            if not args.dry:
                init_job(job=job, raw_args=args.raw_args, spark_conf=args.spark_conf)
                if args.remote_env is not None:
                    job._configure_remote_input(args.remote_env)
                df = job.analyze()
                timeline = timeline_analysis.analyse_timeline(
                    df, reference_time_col=job.reference_time_col, cohort_cols=job.cohorts, nb_buckets=job.nb_buckets
                )
                output_dir = conf.get_analyze_timeline_dir_location()
                if len(job.cohorts) != 1:
                    draw_timeline_analysis.draw(timeline, job.cohorts, output_dir, table)
                else:
                    draw_timeline_analysis.draw(timeline, job.cohorts + [""], output_dir, table)

        return ReturnCode.Success
