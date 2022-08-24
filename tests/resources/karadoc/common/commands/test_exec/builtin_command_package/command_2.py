from argparse import ArgumentParser, Namespace

from karadoc.common.commands.command import Command


class BuiltinCommand2(Command):
    description = "a fake built-in command that does nothing"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        parser.add_argument("--option", dest="option", default=None, type=str, help="a dummy option")

    @staticmethod
    def do_command(args: Namespace):
        # Doing nothing
        pass
