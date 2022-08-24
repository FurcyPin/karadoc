from abc import ABC
from argparse import ArgumentParser

from karadoc.common.commands.command import Command


class ReadFromOption(Command, ABC):
    @staticmethod
    def add_arguments(parser: ArgumentParser) -> None:
        parser.add_argument(
            "--read-from",
            dest="remote_env",
            metavar="ENV",
            help="Read data from a remote warehouse in env ENV instead of the local warehouse",
        )
