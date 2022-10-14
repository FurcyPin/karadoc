from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Set, Tuple

import graphviz

from karadoc.common import table_utils
from karadoc.common.graph.graph_filter import GraphFilter
from karadoc.common.model.table_index import Populate, Table

if TYPE_CHECKING:
    import networkx as nx


def build_graph(table_index: Dict[str, Table]) -> "nx.DiGraph":
    """Generate the full dependency graph of the model
    :return: a directed graph of table names
    """
    import networkx as nx

    graph: nx.DiGraph = nx.DiGraph()
    for table in table_index.values():
        graph.add_node(table.full_name)
        if table.populate is not None:
            for source_full_name in table.populate.input_tables:
                graph.add_edge(source_full_name, table.full_name)
    return graph


def __breadth_first_search(  # NOSONAR
    graph: "nx.DiGraph",
    upwards: bool,
    max_depth: Optional[int],
    starting_nodes: List[str],
    ignored_edges: List[Tuple[str, str]],
    visit_node: Optional[Callable[[str], None]] = None,
    visit_edge: Optional[Callable[[str, str], None]] = None,
):
    """Explore the given 'graph' in the direction specified by the 'upwards' flag, up to a distance of 'max_depth',
    and call the method 'visit_node' on each visited node and 'visit_edge' on each visited edge.
    A depth of 0 or less means no exploration in that direction.
    A depth equal to None means unbounded exploration in that direction.

    :param graph: The initial graph
    :param upwards: Go upstream instead of downstream if set to true
    :param max_depth: The maximal distance to explore
    :param starting_nodes: The nodes from which the exploration is started
    :param visit_node: (Optional) Function of type (node) -> None called for each visited node
    :param visit_edge: (Optional) Function of type (source, dest) -> None called for each visited edge
    :return: nothing but updates new_graph
    """
    depth = 1
    next_to_visit = set(starting_nodes)
    already_seen = set(starting_nodes)

    def visit_neighboring_edges(_node: str) -> None:
        """Visit all the edges starting from this node (or going to this node if upwards=True)"""
        neighbors = graph.predecessors(_node) if upwards else graph.successors(_node)
        for neighbor in neighbors:
            source, dest = (neighbor, _node) if upwards else (_node, neighbor)
            if (source, dest) not in ignored_edges:
                if visit_edge is not None:
                    visit_edge(source, dest)
                if neighbor not in already_seen:
                    already_seen.add(neighbor)
                    next_to_visit.add(neighbor)

    while len(next_to_visit) > 0 and (max_depth is None or depth <= max_depth):
        to_visit = next_to_visit
        next_to_visit = set()
        while len(to_visit) > 0:
            node = to_visit.pop()
            if visit_node is not None:
                visit_node(node)
            visit_neighboring_edges(node)
        depth += 1


def __build_oriented_neighbor_subgraph(
    graph: "nx.DiGraph",
    new_graph: "nx.DiGraph",
    upwards: bool,
    max_depth: Optional[int],
    starting_nodes: List[str],
    ignored_edges: List[Tuple[str, str]],
):
    """Explore the given 'graph' in the direction specified by the 'upwards' flag, up to a distance of 'max_depth',
    and adds the explored nodes and edges to the given 'new_graph'.
    A depth of 0 or less means no exploration in that direction.
    A depth equal to None means unbounded exploration in that direction

    :param graph: The initial graph
    :param new_graph: The resulting graph that will be build (not necessarily empty at the start)
    :param upwards: Go upstream instead of downstream if set to true
    :param max_depth: The maximal distance to explore
    :param starting_nodes: The nodes from which the exploration is started
    :return: nothing but updates new_graph
    """
    for node in starting_nodes:
        new_graph.add_node(node)

    def visit_edge(source: str, dest: str) -> None:
        if not new_graph.has_edge(source, dest):
            new_graph.add_edge(source, dest)

    __breadth_first_search(
        graph,
        upwards,
        max_depth,
        starting_nodes,
        ignored_edges,
        visit_edge=visit_edge,
    )


def build_graph_filters(
    nodes: List[str], default_upstream_depth: Optional[int], default_downstream_depth: Optional[int]
):
    def to_graph_filter(s):
        if type(s) == GraphFilter:
            return s
        else:
            return GraphFilter(s, default_upstream_depth, default_downstream_depth)

    return [to_graph_filter(s) for s in nodes]


def get_filtered_subgraph(
    graph: "nx.DiGraph", graph_filters: List[GraphFilter], ignored_edges: List[Tuple[str, str]]
) -> "nx.DiGraph":
    """Generate a new subgraph of the given 'graph' starting from the specified 'nodes',
        exploring upstream up to a depth of 'upstream_depth' and downstream up to a depth of 'downstream_depth'.
        A depth of 0 or less means no exploration in that direction.
        A depth equal to None means unbounded exploration in that direction

    :param graph:
    :param graph_filters:
    :param ignored_edges:
    :return:
    """
    import networkx as nx

    new_graph: "nx.DiGraph" = nx.DiGraph()
    for graph_filter in graph_filters:
        __build_oriented_neighbor_subgraph(
            graph, new_graph, True, graph_filter.upstream_depth, [graph_filter.node], ignored_edges
        )
        __build_oriented_neighbor_subgraph(
            graph, new_graph, False, graph_filter.downstream_depth, [graph_filter.node], ignored_edges
        )
    return new_graph


def get_orphans(graph: "nx.DiGraph") -> List[str]:
    """Returns the list of nodes without any predecessors.

    :param graph:
    :return:
    """
    res = []
    for node in graph.nodes:
        if next(graph.predecessors(node), 0) == 0:
            res.append(node)
    return res


def find_tables_to_disable(index: Dict[str, Table], graph: "nx.DiGraph") -> List[str]:
    """Find tables that can be disabled. A table can be disabled if all three conditions below are met :
    - it has no external output
    - all of its direct successors are already disabled or can be disabled (recursively)

    :param index:
    :param graph:
    :return:
    """
    reverse_topological_sort = reversed(get_topological_sort(graph))
    dead_tables = {}

    def _is_disabled(populate: Populate):
        return populate.disabled

    def _has_external_outputs(populate: Populate):
        return len(populate.external_outputs) > 0

    def _all_direct_successors_are_dead(table):
        for successor in graph.successors(table):
            if successor not in dead_tables:
                return False
        return True

    for table in reverse_topological_sort:
        table_info = index[table]
        populate = table_info.populate
        if populate is not None:
            if _is_disabled(populate):
                dead_tables[table] = "already disabled"
            elif not _has_external_outputs(populate) and _all_direct_successors_are_dead(table):
                dead_tables[table] = "can be disabled"

    return sorted([table for table, state in dead_tables.items() if state == "can be disabled"])


def get_topological_sort(graph: "nx.DiGraph") -> List[str]:
    import networkx as nx

    return list(nx.algorithms.topological_sort(graph))


def _format_table_node(table: str, table_name: str, populate: Populate, highlight_tables: Set[str]) -> Dict[str, str]:
    """Build the set of format option that will be passed to graphviz for this table node.

    :param table: full name of the table
    :param table_name: short name of the table
    :param populate: populate representing the table
    :param highlight_tables: set of full table names that should be highlighted
    :return: a Dict of formatting options
    """
    node_format = {
        "shape": "box",
        "fillcolor": "aliceblue",
        "label": table_name,
        "style": "filled",
        "color": None,
    }
    if populate is not None:
        if len(populate.partitions) > 0:
            node_format["label"] += "/%s" % "/".join(populate.partitions)
            node_format["fillcolor"] = "bisque"
        if populate.disabled:
            node_format["fillcolor"] = "gray90"
    if table in highlight_tables:
        node_format["style"] += ",bold"
        node_format["color"] = "blue"
    return node_format


def render_graph(
    graph: "nx.DiGraph",
    table_index: Dict[str, Table],
    highlight_tables: Optional[List[str]] = None,
    output_format: str = "png",
) -> graphviz.Digraph:
    """Generates a Digraph.gv file and display it using the Graphviz library"""
    if highlight_tables is None:
        highlight_tables = set()
    else:
        highlight_tables = set(highlight_tables)
    dot = graphviz.Digraph(format=output_format)
    dot.attr(rankdir="LR")
    schema_subgraphs = {}

    def get_schema_subgraph(schema: str) -> graphviz.Digraph:
        sub = schema_subgraphs.get(schema)
        if sub is None:
            sub = graphviz.Digraph(name="cluster_" + schema)
            sub.attr(label=schema)
            sub.attr(style="dotted")
            schema_subgraphs[schema] = sub
        return sub

    for table in graph.nodes():
        table_info = table_index[table]
        (schema_name, table_name, _) = table_utils.parse_table_name(table)
        sub = get_schema_subgraph(schema_name)
        populate: Populate = table_info.populate
        node_format = _format_table_node(table, table_name, populate, highlight_tables)
        sub.node(table, **node_format)
    for (source, dest) in graph.edges():
        dot.edge(source, dest)

    for (schema, sub) in schema_subgraphs.items():
        dot.subgraph(sub)

    dot.render(view=True)

    return dot
