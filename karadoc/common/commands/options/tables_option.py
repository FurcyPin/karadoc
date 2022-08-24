from abc import ABC
from argparse import ArgumentParser

from karadoc.common.commands.command import Command


class TablesOption(Command, ABC):
    @staticmethod
    def add_arguments(parser: ArgumentParser) -> None:
        parser.add_argument(
            "--tables", dest="tables", metavar="table", type=str, nargs="+", required=True, help="Names of the tables"
        )
