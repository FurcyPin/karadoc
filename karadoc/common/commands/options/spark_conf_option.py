from abc import ABC
from argparse import ArgumentParser

from karadoc.common.commands.command import Command


class SparkConfOption(Command, ABC):
    @staticmethod
    def add_arguments(parser: ArgumentParser) -> None:
        from karadoc.common.commands.parsing import StoreDictKeyValue

        parser.add_argument(
            "--spark-conf",
            nargs="*",
            dest="spark_conf",
            metavar="KEY=VALUE",
            required=False,
            action=StoreDictKeyValue,
            help="Specify Spark configuration parameters. Use this only in LOCAL mode. "
            "In distributed mode, please pass them via the spark-submit command.",
        )
