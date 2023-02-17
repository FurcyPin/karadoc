from typing import TYPE_CHECKING, Generator

from karadoc.common.validations import ValidationResult, ValidationResultTemplate, ValidationSeverity

if TYPE_CHECKING:
    import networkx as nx


ValidationResult_GraphContainsCycle = ValidationResultTemplate(
    check_type="karadoc.graph.contains_cycle",
    message_template='Dependency cycle found : {cycle}"',
    default_severity=ValidationSeverity.Critical,
)


def validate_graph(graph: "nx.DiGraph") -> Generator[ValidationResult, None, None]:
    import networkx as nx

    if not nx.is_directed_acyclic_graph(graph):
        yield ValidationResult_GraphContainsCycle(cycle=nx.find_cycle(graph))
