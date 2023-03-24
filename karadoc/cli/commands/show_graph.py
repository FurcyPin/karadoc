from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING

from karadoc.common.commands.command import Command
from karadoc.common.commands.options.tables_option import ModelsOption
from karadoc.common.graph import table_graph
from karadoc.common.model import table_index
from karadoc.common.validations import fail_if_results
from karadoc.common.validations.graph_validations import validate_graph

if TYPE_CHECKING:
    from networkx import DiGraph


def inspect(graph: "DiGraph"):
    """Function used for unit tests. Mock this function to be able to inspect the produced TableGraph
    during the execution of the command."""
    pass


class ShowGraphCommand(Command):
    description = "display the dependency graph for the specified tables"

    @staticmethod
    def add_arguments(parser: ArgumentParser) -> None:
        ModelsOption.add_arguments(parser, required=False)
        parser.add_argument(
            "-b",
            "--before",
            dest="before",
            type=int,
            default=0,
            metavar="DIST",
            nargs="?",
            help="Recursively show upstream dependencies of the specified tables up to a distance of DIST",
        )
        parser.add_argument(
            "-a",
            "--after",
            dest="after",
            type=int,
            default=0,
            metavar="DIST",
            nargs="?",
            help="Recursively show downstream dependencies of the specified tables up to a distance of DIST",
        )
        parser.add_argument(
            "--reduce",
            dest="reduce",
            default=False,
            action="store_true",
            help="Perform a transitive reduction on the graph. For instance if we have A -> B -> C and A -> C, "
            "the dependency A -> C will be dropped",
        )
        parser.add_argument(
            "--show-hidden",
            dest="show_hidden",
            default=False,
            action="store_true",
            help="Display all dependencies, even those hidden by setting `job.hide_inputs = True`",
        )
        parser.add_argument(
            "--output-format",
            dest="output_format",
            default="png",
            type=str,
            help="Output format of the graph (e.g. 'png', 'svg')",
        )
        parser.add_argument(
            "--print-topological-sort", dest="print_topological_sort", default=False, action="store_true", help=""
        )

    @staticmethod
    def do_command(args: Namespace) -> None:
        import networkx as nx

        index = table_index.build_table_index()
        graph = table_graph.build_graph(index)

        # When the --tables argument is omitted, we display the full graph
        if args.models == []:
            args.before = None
            args.after = None
            args.models = [table.full_name for table in index.values()]
            args.models = [table.full_name for table in index.values()]

        if args.reduce:
            graph = nx.transitive_reduction(graph)
        validation_results = validate_graph(graph)

        ignored_edges = []
        if not args.show_hidden:
            ignored_edges = [
                (input_name, table_name)
                for table_name, table in index.items()
                if table.populate is not None and table.populate.hide_inputs
                for input_name in table.populate.input_tables
            ]

        graph_filters = table_graph.build_graph_filters(args.models, args.before, args.after)
        graph = table_graph.get_filtered_subgraph(graph, graph_filters, ignored_edges)

        tables = [filter.node for filter in graph_filters]
        inspect(graph)
        table_graph.render_graph(graph, index, tables, output_format=args.output_format)

        fail_if_results(validation_results)

        if args.print_topological_sort:
            for t in table_graph.get_topological_sort(graph):
                print(t)
