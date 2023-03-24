from argparse import ArgumentParser, Namespace

from karadoc.common.commands.command import Command
from karadoc.common.commands.options.tables_option import ModelsOption


class AnalyzeBQCommand(Command):
    description = "compute an excel report with descriptive statistics on bigquery tables"

    @staticmethod
    def add_arguments(parser: ArgumentParser) -> None:
        ModelsOption.add_arguments(parser)
        parser.add_argument(
            "--output",
            dest="output",
            type=str,
            default=None,
            metavar="PATH",
            help="Path of the .xlsx file where the results of the command will be written",
        )
        parser.add_argument(
            "--env",
            dest="env",
            type=str,
            default="bigquery",
            metavar="ENV_NAME",
            help="Use credential file from the specified bigquery connection",
        )

    @staticmethod
    def do_command(args: Namespace) -> None:
        # We do this import here to avoid loading unnecessary libraries when running other commands
        from karadoc.biqguery.analyze import analyze_tables

        analyze_tables(args.models, args.env, args.output)
