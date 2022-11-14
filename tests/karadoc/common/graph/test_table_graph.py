from typing import List, Optional, Tuple
from unittest import TestCase, mock

import networkx as nx

from karadoc.common.graph import table_graph
from karadoc.test_utils.mock_settings import mock_settings_for_test_class
from karadoc.test_utils.strings import strip_margin
from tests.karadoc.test_utils import get_resource_folder_path


def mock_render(*args, **kwargs):
    """This mock prevent us from calling the program 'dot' which is not installed on the CI hosts."""
    pass


def build_test_graph() -> nx.DiGraph:
    graph = nx.DiGraph()
    nodes = ["schema.t0", "schema.t1", "schema.t2", "schema.t12"]
    edges = [
        ("schema.t0", "schema.t1"),
        ("schema.t0", "schema.t2"),
        ("schema.t1", "schema.t12"),
        ("schema.t2", "schema.t12"),
    ]
    graph.add_nodes_from(nodes)
    graph.add_edges_from(edges)
    return graph


def get_oriented_neighbor_subgraph(
    graph: nx.DiGraph,
    upstream_depth: Optional[int],
    downstream_depth: Optional[int],
    starting_nodes: List[str],
    ignored_edges: List[Tuple[str, str]] = None,
) -> nx.DiGraph:
    """Generate a new subgraph of the given 'graph' starting from the specified 'nodes',
        exploring upstream up to a depth of 'upstream_depth' and downstream up to a depth of 'downstream_depth'.
        A depth of 0 or less means no exploration in that direction.
        A depth equal to None means unbounded exploration in that direction

    :param graph:
    :param upstream_depth:
    :param downstream_depth:
    :param starting_nodes:
    :param ignored_edges:
    :return:
    """
    if ignored_edges is None:
        ignored_edges = []
    graph_filters = table_graph.build_graph_filters(starting_nodes, upstream_depth, downstream_depth)
    return table_graph.get_filtered_subgraph(graph, graph_filters, ignored_edges)


@mock_settings_for_test_class(
    {"enable_file_index_cache": False, "model_dir": get_resource_folder_path(__name__) + "/model"}
)
class TestTableGraph(TestCase):
    def test_build_graph(self):
        from karadoc.common.model.table_index import build_table_index

        table_index = build_table_index()
        actual = table_graph.build_graph(table_index)
        expected = build_test_graph()
        self.assertSetEqual(set(actual.nodes), set(expected.nodes))
        self.assertSetEqual(set(actual.edges), set(expected.edges))

    def test_get_predecessor_subgraph(self):
        graph = build_test_graph()
        subgraph = get_oriented_neighbor_subgraph(graph, None, 0, ["schema.t1"])
        self.assertSetEqual(set(subgraph.nodes), {"schema.t0", "schema.t1"})
        self.assertSetEqual(set(subgraph.edges), {("schema.t0", "schema.t1")})

    def test_get_predecessor_subgraph_with_max_upstream_distance(self):
        graph = build_test_graph()
        subgraph = get_oriented_neighbor_subgraph(graph, 1, 0, ["schema.t12"])
        self.assertSetEqual(set(subgraph.nodes), {"schema.t1", "schema.t2", "schema.t12"})
        self.assertSetEqual(set(subgraph.edges), {("schema.t1", "schema.t12"), ("schema.t2", "schema.t12")})

    def test_get_successor_subgraph(self):
        graph = build_test_graph()
        subgraph = get_oriented_neighbor_subgraph(graph, 0, None, ["schema.t2"])
        self.assertSetEqual(set(subgraph.nodes), {"schema.t2", "schema.t12"})
        self.assertSetEqual(set(subgraph.edges), {("schema.t2", "schema.t12")})

    def test_get_successor_subgraph_with_max_downstream_distance(self):
        graph = build_test_graph()
        subgraph = get_oriented_neighbor_subgraph(graph, 0, 1, ["schema.t0"])
        self.assertSetEqual(set(subgraph.nodes), {"schema.t0", "schema.t1", "schema.t2"})
        self.assertSetEqual(set(subgraph.edges), {("schema.t0", "schema.t1"), ("schema.t0", "schema.t2")})

    def test_get_oriented_neighbor_subgraph_both_directions(self):
        graph = build_test_graph()
        subgraph = get_oriented_neighbor_subgraph(graph, None, None, ["schema.t1"])
        self.assertSetEqual(set(subgraph.nodes), {"schema.t0", "schema.t1", "schema.t12"})
        self.assertSetEqual(set(subgraph.edges), {("schema.t0", "schema.t1"), ("schema.t1", "schema.t12")})

    def test_get_oriented_neighbor_subgraph_with_ignored_edges(self):
        graph = build_test_graph()
        subgraph = get_oriented_neighbor_subgraph(
            graph, None, None, ["schema.t1"], ignored_edges=[("schema.t1", "schema.t12")]
        )
        self.assertSetEqual(set(subgraph.nodes), {"schema.t0", "schema.t1"})
        self.assertSetEqual(set(subgraph.edges), {("schema.t0", "schema.t1")})

    def test_get_oriented_neighbor_subgraph_with_upstream_filters(self):
        graph = build_test_graph()
        subgraph = get_oriented_neighbor_subgraph(graph, 0, 0, ["schema.t0+1"])
        self.assertSetEqual(set(subgraph.nodes), {"schema.t0", "schema.t1", "schema.t2"})
        self.assertSetEqual(set(subgraph.edges), {("schema.t0", "schema.t1"), ("schema.t0", "schema.t2")})

    def test_get_oriented_neighbor_subgraph_with_bidirectional_filters(self):
        graph = build_test_graph()
        subgraph = get_oriented_neighbor_subgraph(graph, 0, 0, ["schema.t1+1", "1+schema.t2"])
        self.assertSetEqual(set(subgraph.nodes), {"schema.t0", "schema.t1", "schema.t2", "schema.t12"})
        self.assertSetEqual(set(subgraph.edges), {("schema.t0", "schema.t2"), ("schema.t1", "schema.t12")})

    def test_get_oriented_neighbor_subgraph_with_depth_2(self):
        graph = build_test_graph()
        subgraph = get_oriented_neighbor_subgraph(graph, 0, 0, ["2+schema.t12"])
        self.assertSetEqual(set(subgraph.nodes), {"schema.t0", "schema.t1", "schema.t2", "schema.t12"})
        self.assertSetEqual(
            set(subgraph.edges),
            {
                ("schema.t0", "schema.t1"),
                ("schema.t0", "schema.t2"),
                ("schema.t1", "schema.t12"),
                ("schema.t2", "schema.t12"),
            },
        )

    def test_get_orphans(self):
        graph = build_test_graph()
        actual = table_graph.get_orphans(graph)
        self.assertListEqual(actual, ["schema.t0"])

    def test_get_topological_sort(self):
        graph = build_test_graph()
        actual = table_graph.get_topological_sort(graph)
        self.assertEqual(actual[0], "schema.t0")
        self.assertEqual(actual[3], "schema.t12")
        self.assertSetEqual(set(actual[1:3]), {"schema.t1", "schema.t2"})

    @mock.patch("graphviz.Digraph.render", mock_render)
    def test_render_graph(self):
        from karadoc.common.model.table_index import build_table_index

        table_index = build_table_index()
        graph = table_graph.build_graph(table_index)
        dot = table_graph.render_graph(graph, table_index)
        expected_edges = [
            '"schema.t0" -> "schema.t1"',
            '"schema.t0" -> "schema.t2"',
            '"schema.t1" -> "schema.t12"',
            '"schema.t2" -> "schema.t12"',
        ]
        expected_nodes = [
            '"schema.t0" [label=t0 fillcolor=aliceblue shape=box style=filled]',
            '"schema.t1" [label=t1 fillcolor=aliceblue shape=box style=filled]',
            '"schema.t2" [label=t2 fillcolor=aliceblue shape=box style=filled]',
            '"schema.t12" [label=t12 fillcolor=aliceblue shape=box style=filled]',
        ]
        expected = strip_margin(
            """
            |digraph {
            |\trankdir=LR
            |\t{expectedEdge}
            |\t{expectedEdge}
            |\t{expectedEdge}
            |\t{expectedEdge}
            |\tsubgraph cluster_schema {
            |\t\tlabel=schema
            |\t\tstyle=dotted
            |\t\t{expected_node}
            |\t\t{expected_node}
            |\t\t{expected_node}
            |\t\t{expected_node}
            |\t}
            |}"""
        )
        str_dot = str(dot)
        for expectedEdge in expected_edges:
            self.assertIn(expectedEdge, str_dot)
            str_dot = str_dot.replace(expectedEdge, "{expectedEdge}")
        for expected_node in expected_nodes:
            self.assertIn(expected_node, str_dot)
            str_dot = str_dot.replace(expected_node, "{expected_node}")
        self.maxDiff = None
        self.assertEqual(str_dot, expected)


@mock_settings_for_test_class(
    {
        "enable_file_index_cache": False,
        "model_dir": get_resource_folder_path(__name__) + "/model_graph_partitions",
    }
)
class TestTableGraphPartitions(TestCase):
    @mock.patch("graphviz.Digraph.render", mock_render)
    def test_render_graph_partitions(self):
        from karadoc.common.model.table_index import build_table_index

        table_index = build_table_index()
        graph = table_graph.build_graph(table_index)
        dot = table_graph.render_graph(graph, table_index)
        expected_nodes = [
            '"partitions.static_partition" [label="static_partition/day" fillcolor=bisque shape=box style=filled]',
            '"partitions.no_partition" [label=no_partition fillcolor=aliceblue shape=box style=filled]',
            '"partitions.dynamic_partition" [label="dynamic_partition/day" fillcolor=bisque shape=box style=filled]',
        ]
        expected = """digraph {
        |\trankdir=LR
        |\tsubgraph cluster_partitions {
        |\t\tlabel=partitions
        |\t\tstyle=dotted
        |\t\t{expected_node}
        |\t\t{expected_node}
        |\t\t{expected_node}
        |\t}
        |}""".replace(
            "        |", ""
        )
        str_dot = str(dot)
        for expected_node in expected_nodes:
            self.assertIn(expected_node, str_dot)
            str_dot = str_dot.replace(expected_node, "{expected_node}")
        self.maxDiff = None
        self.assertEqual(str_dot, expected)
