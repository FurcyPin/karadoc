from abc import ABC
from argparse import ArgumentParser

from karadoc.common.commands.command import Command


class VarsOption(Command, ABC):
    @staticmethod
    def add_arguments(parser: ArgumentParser) -> None:
        from karadoc.common.commands.parsing import StoreDictKeyValue

        parser.add_argument(
            "--vars",
            nargs="*",
            dest="vars",
            metavar="KEY=VALUE",
            required=False,
            action=StoreDictKeyValue,
            help="Set values for job variables",
        )
