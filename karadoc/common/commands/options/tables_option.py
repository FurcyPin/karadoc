from abc import ABC
from argparse import ArgumentParser

from karadoc.common.commands.command import Command
from karadoc.common.model.file_index import list_schema_table_folders


class TablesOption(Command, ABC):
    @staticmethod
    def add_arguments(parser: ArgumentParser, required=True) -> None:
        parser.add_argument(
            "--tables",
            dest="tables",
            metavar="table",
            type=str,
            nargs="+",
            required=required,
            help="Names of the tables",
            default=[],
        ).completer = TablesOption.option_completer

    @staticmethod
    def option_completer(prefix: str, **kwargs):
        split_prefix = prefix.split(".")
        schema_prefix = split_prefix[0]
        table_prefix = None
        result = set()
        if "." in prefix:
            table_prefix = split_prefix[1]
        for schema, table, folder in list_schema_table_folders():
            schema: str
            table: str
            if table_prefix is None:
                if schema.startswith(schema_prefix):
                    result.add(f"{schema}.")
            else:
                if schema == schema_prefix and table.startswith(table_prefix):
                    result.add(f"{schema}.{table}")
        return list(result)
