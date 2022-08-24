from argparse import ArgumentParser, Namespace

from karadoc.common.commands.command import Command


class OverrideCommand1(Command):
    description = "a custom command that overrides a builtin command"

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
        # Doing nothing
        pass
