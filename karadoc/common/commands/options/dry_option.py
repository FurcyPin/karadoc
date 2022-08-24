from abc import ABC
from argparse import ArgumentParser

from karadoc.common.commands.command import Command


class DryOption(Command, ABC):
    @staticmethod
    def add_arguments(parser: ArgumentParser) -> None:
        parser.add_argument("--dry", dest="dry", default=False, action="store_true", help="Perform a dry-run")
