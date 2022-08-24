from argparse import ArgumentParser
from typing import List, Tuple

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from karadoc.common import Job, run
from karadoc.common.commands.command import Command
from karadoc.common.commands.options.read_from_option import ReadFromOption
from karadoc.common.commands.options.tables_option import TablesOption
from karadoc.common.commands.return_code import ReturnCode
from karadoc.common.commands.spark import init_job
from karadoc.common.quality import CheckSeverity, alert
from karadoc.common.quality.exec import execute_alert
from karadoc.common.spark_utils import quote_idempotent, unquote


def _get_key_columns(job: Job):
    """Returns the set of all flat keys used as part of primary or secondary keys. It does not contain duplicate names

    >>> from karadoc.common import Job
    >>> job = Job()
    >>> _get_key_columns(job)
    []
    >>> job.primary_key = 'abc'
    >>> _get_key_columns(job)
    ['abc']
    >>> job.primary_key = ("c1", "c2", "c3")
    >>> _get_key_columns(job)
    ['c1', 'c2', 'c3']
    >>> job.secondary_keys = [("c1", "c4"), "c2"]
    >>> _get_key_columns(job)
    ['c1', 'c2', 'c3', 'c4']

    :param job:
    :return:
    """
    key = job.standardized_primary_key
    if key is None:
        key = ()
    # Hack: unlike sets, dict preserves ordering on keys.
    return list({column: None for tup in [key] + job.standardized_secondary_keys for column in tup}.keys())


def _get_keys(job: Job) -> List[Tuple[str, ...]]:
    """Returns the set of all columns used as part of primary or secondary keys

    >>> from karadoc.common import Job
    >>> job = Job()
    >>> _get_keys(job)
    []
    >>> job.primary_key = 'c1'
    >>> _get_keys(job)
    [('c1',)]
    >>> job.primary_key = ('c1', 'c2')
    >>> _get_keys(job)
    [('c1', 'c2')]
    >>> job.secondary_keys = [('c1', 'c3'), 'c4']
    >>> _get_keys(job)
    [('c1', 'c2'), ('c1', 'c3'), ('c4',)]

    :param job:
    :return:
    """
    key = job.standardized_primary_key
    if key is None:
        key = []
    else:
        key = [key]
    # Hack: unlike sets, dict preserves ordering on keys.
    return list({tup: None for tup in key + job.standardized_secondary_keys}.keys())


class NoKeyDefinedException(Exception):
    """
    Typed exception, to be raised if there is no key defined on this table.
    """

    def __init__(self, table: str):
        """
        :param table: keyless table
        """
        super().__init__("ERROR: No key is defined for the job")
        self.table = table


def _check_key_uniqueness(df: DataFrame, key: Tuple[str, ...], table: str, output_alert_table: str):
    @alert(
        description="Check that the following key has no duplicates: %s" % str(key),
        severity=CheckSeverity.Critical,
        name=f"check_key_uniqueness:{key}",
    )
    def check_key_uniqueness() -> DataFrame:
        key_cols_aliased = [f.col(c).alias(unquote(c)) for c in key]
        key_cols = [quote_idempotent(c) for c in key]

        df1 = df.groupby(*list(key_cols_aliased)).count().filter("count > 1").orderBy(f.desc("count"))
        return df1.select(*list(key_cols), *[f.col("count").alias("nb_duplicates")])

    execute_alert(table, check_key_uniqueness, {}, output_alert_table)


def _check_keys_uniqueness(df: DataFrame, keys: List[Tuple[str, ...]], table: str, output_alert_table: str):
    if len(keys) == 0:
        raise NoKeyDefinedException(table=table)
    for key in keys:
        _check_key_uniqueness(df, key, table, output_alert_table)


def _check_non_null_keys(df: DataFrame, key_columns: List[str], table: str, output_alert_table: str):
    @alert(
        description="Checks that the following columns are never null: %s" % str(key_columns),
        severity=CheckSeverity.Critical,
    )
    def check_non_null_keys():
        """Checks if a key has null values in a dataframe.
        If the key is composite, we check that every element is non-null
        Example:
                key = ('user_id', 'channel_id')
                The function checks that neither user_id nor channel_id has null values
        """
        null_keys_array = f.array(*[f.when(f.col(k).isNull(), f.lit(k)) for k in key_columns])
        res_df = (
            df.select(null_keys_array.alias("null_keys"))
            .select(f.expr("FILTER(null_keys, x -> x IS NOT NULL)").alias("null_keys"))
            .where("SIZE(null_keys) > 0")
        )
        return res_df

    execute_alert(table, check_non_null_keys, {}, output_alert_table)


class CheckKeysCommand(Command):
    description = "check that the table's primary and secondary keys are unique and non-nulls"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        TablesOption.add_arguments(parser)
        ReadFromOption.add_arguments(parser)

        parser.add_argument(
            "--output-alert-table",
            dest="output_alert_table",
            default=None,
            required=False,
            type=str,
            metavar="TABLE",
            help="Write the generated check keys DataFrames to the outputs and external_outputs of the "
            "specified table. This table must have a valid POPULATE.py file, with a run() method that "
            "will be ignored.",
        )

    @staticmethod
    def do_command(args) -> ReturnCode:
        for table in args.tables:
            job = run.load_populate(table)
            init_job(job=job, raw_args=args.raw_args)
            if args.remote_env is not None:
                job._configure_remote_input(args.remote_env)
            print("Check keys started for table %s" % table)

            job.inputs = {"source": table}
            df = job.read_table("source")

            keys = _get_keys(job)
            _check_keys_uniqueness(df, keys, table, args.output_alert_table)

            key_columns = _get_key_columns(job)
            _check_non_null_keys(df, key_columns, table, args.output_alert_table)

        return ReturnCode.Success
