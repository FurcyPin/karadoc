import os
import sys
from typing import Dict

import graphviz
import networkx as nx
from airflow.models import DAG, BaseOperator, DagBag

from karadoc.airflow.conf import (
    get_airflow_dag_folder_location,
    get_airflow_plugin_folder_location,
)
from karadoc.airflow.utils import is_external_sensor, is_trigger


class DagNotFoundException(Exception):
    def __init__(self, dag_id: str) -> None:
        super().__init__(f"Dag not found : {dag_id}")
        self.dag_id = dag_id


class DagLoadingException(Exception):
    pass


def variable_get_mock(*args, **kwargs):
    return ""


def __delete_airflow_modules() -> None:
    """Delete all airflow modules to force Airflow to reload the environment variables AIRFLOW__CORE__PLUGINS_FOLDER
    and AIRFLOW__CORE__DAGS_FOLDER when we update them.
    """
    template_package_name = "airflow"
    template_modules = [m for m in sys.modules if m.startswith(template_package_name)]
    for m in template_modules:
        del sys.modules[m]


def build_dag_bag() -> DagBag:
    __delete_airflow_modules()
    plugins_folder = get_airflow_plugin_folder_location()
    dag_folder = get_airflow_dag_folder_location()
    os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = plugins_folder
    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = dag_folder
    return DagBag(dag_folder=dag_folder, include_examples=False)


def get_orphan_tasks(dag: DAG):
    return [task for task in dag.tasks if len(task.upstream_list) == 0]


def task_full_name(task: BaseOperator):
    return task.dag_id + "." + task.task_id


def _get_dag(dag_bag: DagBag, dag_id: str) -> DAG:
    if dag_id not in dag_bag.dags:
        raise DagNotFoundException(dag_id)
    trigger_dag = dag_bag.dags[dag_id]
    return trigger_dag


def build_task_dependency_graph(graph: nx.DiGraph, dag: DAG, dag_bag: DagBag) -> nx.DiGraph:
    """Builds the dependency graph of all airflow tasks.
    It is able to display cross-dags dependencies by analyzing tasks of type CustomTriggerDagRunOperator
    and ExternalTaskSensor.

    :param graph:
    :param dag:
    :param dag_bag:
    :return:
    """
    for task in dag.tasks:
        try:
            graph.add_node(task_full_name(task))
            for successor in task.downstream_list:
                graph.add_edge(task_full_name(task), task_full_name(successor))
            if is_trigger(task):
                trigger_dag = _get_dag(dag_bag, task.trigger_dag_id)
                for starting_task in get_orphan_tasks(trigger_dag):
                    graph.add_edge(task_full_name(task), task_full_name(starting_task))
            if is_external_sensor(task):
                external_dag = _get_dag(dag_bag, task.external_dag_id)
                external_task = external_dag.get_task(task.external_task_id)
                graph.add_edge(task_full_name(external_task), task_full_name(task))
        except Exception as e:
            raise DagLoadingException(f"Failed to load task {task_full_name(task)} :\n" + e.args[0]) from e
    return graph


def build_graph(dag_bag: DagBag) -> nx.DiGraph:
    """Generate the full dependency graph of all airflow dags
    :return: a directed graph of full task ids ("dag_id.task_id")
    """
    graph = nx.DiGraph()  # type: nx.DiGraph
    for dag in dag_bag.dags.values():
        graph = build_task_dependency_graph(graph, dag, dag_bag)
    return graph


def transitive_closure(graph) -> nx.DiGraph:
    """Returns the transitive closure of a directed graph

    The transitive closure of G = (V,E) is a graph G+ = (V,E+) such that
    for all v,w in V there is an edge (v,w) in E+ if and only if there
    is a non-null path from v to w in G.

    Parameters
    ----------
    G : NetworkX DiGraph
        A directed graph

    Returns
    -------
    NetworkX DiGraph
        The transitive closure of `G`

    Raises
    ------
    NetworkXNotImplemented
        If `G` is not directed

    References
    ----------
    .. [1] http://www.ics.uci.edu/~eppstein/PADS/PartialOrder.py

    """
    return nx.transitive_closure(graph)


def render_graph(graph: nx.DiGraph):
    """Generates a Digraph.gv file and display it using the Graphviz library"""
    dot = graphviz.Digraph(format="png")
    dot.attr(rankdir="LR")

    dag_subgraphs: Dict[str, graphviz.Digraph] = {}

    def get_dag_subgraph(dag_id: str) -> graphviz.Digraph:
        sub = dag_subgraphs.get(dag_id)
        if sub is None:
            sub = graphviz.Digraph(name="cluster_" + dag_id)
            sub.attr(label=dag_id)
            sub.attr(style="dotted")
            dag_subgraphs[dag_id] = sub
        return sub

    for node in graph.nodes():
        dag_id, task_id = node.split(".", maxsplit=1)
        sub = get_dag_subgraph(dag_id)
        shape = "box"
        fillcolor = "aliceblue"
        label = task_id
        style = "filled"
        color = None
        sub.node(node, label=label, shape=shape, fillcolor=fillcolor, style=style, color=color)
    for (source, dest) in graph.edges():
        dot.edge(source, dest)

    for (schema, sub) in dag_subgraphs.items():
        dot.subgraph(sub)

    dot.render(view=True)


def build_dag_graph(task_graph: nx.DiGraph) -> nx.DiGraph:
    dag_graph = nx.DiGraph()  # type: nx.DiGraph
    for node in task_graph.nodes:
        node_dag = node.split(".", maxsplit=1)[0]
        dag_graph.add_node(node_dag)
    for source, dest in task_graph.edges:
        source_dag = source.split(".", maxsplit=1)[0]
        dest_dag = dest.split(".", maxsplit=1)[0]
        dag_graph.add_edge(source_dag, dest_dag)
    return dag_graph


def render_dag_graph(graph: nx.DiGraph):
    """Generates a Digraph.gv file and display it using the Graphviz library"""
    dot = graphviz.Digraph(format="png")
    dot.attr(rankdir="LR")

    nodes = {node for node in graph.nodes()}
    edges = {(source, dest) for source, dest in graph.edges() if source != dest}
    for node in nodes:
        shape = "ellipse"
        fillcolor = "aliceblue"
        label = node
        style = "filled"
        color = None
        dot.node(node, label=label, shape=shape, fillcolor=fillcolor, style=style, color=color)
    for (source, dest) in edges:
        dot.edge(source, dest)

    dot.render(view=True)
