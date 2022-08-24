from argparse import ArgumentParser, Namespace

from karadoc.common.commands.command import Command


class ShowAirflowDagGraphCommand(Command):
    description = "display the dependency graph of all airflow dags"

    @staticmethod
    def add_arguments(parser: ArgumentParser) -> None:
        # This command takes no arguments
        pass

    @staticmethod
    def do_command(args: Namespace) -> None:
        from karadoc.airflow.dag_graph import build_dag_bag

        dag_bag = build_dag_bag()

        from karadoc.airflow.dag_graph import (
            build_dag_graph,
            build_graph,
            render_dag_graph,
        )

        graph = build_graph(dag_bag)
        dag_graph = build_dag_graph(graph)
        render_dag_graph(dag_graph)
