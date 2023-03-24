import argparse
import logging
from abc import ABC
from argparse import ArgumentParser
from typing import List

from karadoc.common.commands.command import Command
from karadoc.common.model.file_index import list_schema_table_folders
from karadoc.common.observability.console_event import ConsoleEvent

LOG = logging.getLogger(__name__)


class TablesOptionDeprecateAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        LOG.warning(
            ConsoleEvent(
                "WARNING: Option '--tables' is deprecated. Please use '--models' instead.",
                event_type="deprecated_cli_option",
            )
        )
        setattr(namespace, self.dest, values)


class ModelsOption(Command, ABC):
    @staticmethod
    def add_arguments(parser: ArgumentParser, required=True) -> None:
        group = parser.add_mutually_exclusive_group(required=required)
        group.add_argument(
            "--tables",
            dest="models",
            metavar="model",
            action=TablesOptionDeprecateAction,
            type=str,
            nargs="+",
            required=False,
            help="(DEPRECATED: use --models instead)",
            default=[],
        ).completer = ModelsOption.option_completer
        group.add_argument(
            "--models",
            dest="models",
            metavar="model",
            type=str,
            nargs="+",
            required=False,
            help="Names of the models",
            default=[],
        ).completer = ModelsOption.option_completer

    @staticmethod
    def option_completer(prefix: str, **kwargs) -> List[str]:
        split_prefix = prefix.split(".")
        schema_prefix = split_prefix[0]
        table_prefix = None
        result = set()
        if "." in prefix:
            table_prefix = split_prefix[1]
        schema: str
        table: str
        for schema, table, folder in list_schema_table_folders():
            if table_prefix is None:
                if schema.startswith(schema_prefix):
                    result.add(f"{schema}.")
            else:
                if schema == schema_prefix and table.startswith(table_prefix):
                    result.add(f"{schema}.{table}")
        return list(result)
