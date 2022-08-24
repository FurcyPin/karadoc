from typing import Generator

import networkx as nx

from karadoc.common.validations import (
    ValidationResult,
    ValidationResultTemplate,
    ValidationSeverity,
)

ValidationResult_GraphContainsCycle = ValidationResultTemplate(
    check_type="karadoc.graph.contains_cycle",
    message_template='Dependency cycle found : {cycle}"',
    default_severity=ValidationSeverity.Critical,
)


def validate_graph(graph: nx.DiGraph) -> Generator[ValidationResult, None, None]:
    if not nx.is_directed_acyclic_graph(graph):
        yield ValidationResult_GraphContainsCycle(cycle=nx.find_cycle(graph))
