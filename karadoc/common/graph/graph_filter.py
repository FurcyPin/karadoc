import re
from typing import Optional, Tuple

filter_parsing_pattern = re.compile(r"""(?:(\d*)([+]))?([^+]+)(?:([+])(\d*))?""")

s = "+schema.table+2"


def parse_filter_string(filter_str: str) -> Tuple[str, int, int]:
    """Parse a graph filter string and return a tuple (model_name, upstream_depth, downstream_depth)

    A depth of 0 or less means no exploration in that direction
    A depth equal to None means unbounded exploration in that direction

    >>> parse_filter_string("schema.table")
    ('schema.table', 0, 0)
    >>> parse_filter_string("schema.table+")
    ('schema.table', 0, None)
    >>> parse_filter_string("+schema.table")
    ('schema.table', None, 0)
    >>> parse_filter_string("+schema.table+")
    ('schema.table', None, None)
    >>> parse_filter_string("+schema.table+2")
    ('schema.table', None, 2)
    >>> parse_filter_string("3+schema.table+2")
    ('schema.table', 3, 2)

    :param filter_str:
    :return: a tuple (model_name, upstream_depth, downstream_depth)
    """
    groups = filter_parsing_pattern.match(filter_str).groups()

    def get_exploration_depth(is_enabled: bool, str_depth: Optional[str]) -> Optional[int]:
        """Return the parsed value of exploration depth.

        - "" means no exploration -> 0
        - "+" means unbounded exploration -> None
        - "+N" means exploration bounded to N -> N
        """
        if is_enabled:
            if str_depth:
                return int(str_depth)
            else:
                return None
        else:
            return 0

    if len(groups) == 5:
        (upstream_depth, is_upstream, table_name, is_downstream, downstream_depth) = groups
        upstream_depth = get_exploration_depth(is_upstream, upstream_depth)
        downstream_depth = get_exploration_depth(is_downstream, downstream_depth)
        return table_name, upstream_depth, downstream_depth
    else:
        raise ValueError(f"Could not parse the graph filter {s}")


class GraphFilter:
    node: str
    upstream_depth: Optional[int]
    downstream_depth: Optional[int]

    def __init__(self, filter: str, default_upstream_depth: Optional[int], default_downstream_depth: Optional[int]):
        self.node, self.upstream_depth, self.downstream_depth = parse_filter_string(filter)
        if self.upstream_depth == 0:
            self.upstream_depth = default_upstream_depth
        if self.downstream_depth == 0:
            self.downstream_depth = default_downstream_depth
