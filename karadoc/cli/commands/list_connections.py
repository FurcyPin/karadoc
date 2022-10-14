from argparse import ArgumentParser, Namespace

from karadoc.common.commands.command import Command
from karadoc.common.commands.return_code import ReturnCode


class ListConnectionsCommand(Command):
    description = "list all connections"

    @staticmethod
    def add_arguments(parser: ArgumentParser) -> None:
        parser.add_argument(
            "--env", dest="env", metavar="ENV", type=str, help="List the connections on the specified environment"
        )
        parser.add_argument(
            "--format",
            dest="format",
            default="markdown",
            type=str,
            choices=["markdown", "csv", "xlsx"],
            help="(Default: markdown) Output format for the command results.",
        )
        parser.add_argument(
            "--output",
            dest="output",
            type=str,
            default=None,
            metavar="PATH",
            help="Path of the file where the results of the command will be written. "
            "By default, the results are written on stdout, except for binary format (e.g. xlsx) "
            "for which the default is connections.FORMAT",
        )

    @staticmethod
    def do_command(args: Namespace) -> ReturnCode:
        from karadoc.common.model.connection_index import list_connections_df
        from karadoc.common.output.local_export import local_export_dataframe

        binary_formats = ["xlsx"]
        if args.output is None and args.format in binary_formats:
            args.output = f"connections.{args.format}"

        df = list_connections_df(args.env)
        local_export_dataframe(df=df, output=args.output, format=args.format)
        return ReturnCode.Success
