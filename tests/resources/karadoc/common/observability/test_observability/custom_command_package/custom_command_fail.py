from argparse import ArgumentParser, Namespace

from karadoc.common.commands.command import Command


class FailingCommand(Command):
    description = "a custom failing command"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--custom-option",
            dest="custom_option",
            default=None,
            type=str,
            help="a dummy option, different from the option of the original built-in commands",
        )

    @staticmethod
    def do_command(args: Namespace):
        raise ValueError("Fail")
