from argparse import ArgumentParser, Namespace
from itertools import groupby
from typing import List

from karadoc.common import table_utils
from karadoc.common.commands.command import Command
from karadoc.common.model import file_index
from karadoc.common.run import load_populate, print_job_properties


def list_tables():
    for (_, groups) in groupby(file_index.list_schema_table_folders(), lambda x: x[0]):
        for (schema, table, path) in groups:
            full_table_name = "%s.%s" % (schema, table)
            yield full_table_name


def list_tables_with_details():
    for full_table_name in list_tables():
        if table_utils.populate_exists(full_table_name):
            job = load_populate(full_table_name)
            print_job_properties(job)
            print()


def having_keys(full_table_name):
    if table_utils.populate_exists(full_table_name) and (
        load_populate(full_table_name).primary_key is not None or load_populate(full_table_name).secondary_keys != []
    ):
        return True
    else:
        return False


def is_disabled(full_table_name):
    return load_populate(full_table_name).disable


def having_quality_check(full_table_name):
    if table_utils.quality_check_exists(full_table_name):
        return True
    else:
        return False


def get_filter_functions(args):
    if args.with_keys:
        yield having_keys
    if args.is_disabled:
        yield is_disabled
    if args.has_quality_check:
        yield having_quality_check


def list_tables_having_indicated_details(args: List[str]):
    tables = [full_table_name for full_table_name in list_tables()]
    for fun in get_filter_functions(args):
        tables = filter(fun, tables)
    return sorted(tables)


class ListCommand(Command):
    description = "list all tables"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--with-keys", dest="with_keys", default=False, action="store_true", help="list tables with indicated keys"
        )
        parser.add_argument(
            "--is-disabled", dest="is_disabled", default=False, action="store_true", help="list disabled tables"
        )
        parser.add_argument(
            "--has-quality-check",
            dest="has_quality_check",
            default=False,
            action="store_true",
            help="list tables having quality_checks",
        )
        parser.add_argument("--output", dest="output", metavar="output", type=str, help="generate an output file")

    @staticmethod
    def do_command(args: Namespace):
        tables = list_tables_having_indicated_details(args)
        if args.output is not None:
            with open(args.output, "w") as out:
                for r in tables:
                    out.write("{}\n".format(r))
        else:
            for r in tables:
                print(r)
