from argparse import ArgumentParser, Namespace
from typing import List

import networkx as nx
from pyspark import Row
from pyspark.sql import DataFrame

from karadoc.common.commands.command import Command
from karadoc.common.graph import table_graph
from karadoc.common.graph.table_graph import find_tables_to_disable
from karadoc.common.model import table_index
from karadoc.common.output.local_export import local_export_dataframe
from karadoc.common.validations import fail_if_results
from karadoc.common.validations.graph_validations import validate_graph


def _str_list_to_df(data: List[str]) -> DataFrame:
    """Builds a Spark DataFrame out of a List[str]."""
    from karadoc.common import Job

    job = Job()
    job.init()
    df = job.spark.createDataFrame([Row(t) for t in data], schema="full_table_name STRING")
    df = df.orderBy("full_table_name")
    return df


class FindTablesToDisableCommand(Command):
    description = (
        "(experimental) find tables that can be disabled. A table can be disabled if it "
        "has no external output and if all of its direct successors are already disabled or can be disabled."
    )

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--format",
            dest="format",
            default="txt",
            choices=["txt", "markdown", "csv", "xlsx"],
            help="(Default: txt) Output format for the command results.",
        )
        parser.add_argument(
            "--output",
            dest="output",
            default=None,
            metavar="PATH",
            help="Path of the file where the results of the command will be written. "
            "By default, the results are written on stdout, except for binary format (e.g. xlsx) "
            "for which the default is connections.FORMAT",
        )

    @staticmethod
    def do_command(args: Namespace):
        binary_formats = ["xlsx"]
        if args.output is None and args.format in binary_formats:
            args.output = f"connections.{args.format}"

        index = table_index.build_table_index()
        graph = table_graph.build_graph(index)
        validation_results = validate_graph(graph)

        graph: nx.DiGraph = nx.transitive_reduction(graph)

        tables_to_disable = find_tables_to_disable(index, graph)
        df = _str_list_to_df(tables_to_disable)
        local_export_dataframe(df=df, output=args.output, format=args.format)

        fail_if_results(validation_results)
