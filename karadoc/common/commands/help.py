import textwrap
from argparse import HelpFormatter
from typing import List


class LineBreakHelpFormatter(HelpFormatter):
    """Help message formatter which preserves line breaks descriptions."""

    def _split_lines(self, text: str, width: int) -> List[str]:
        res = []
        for lines in text.split("\n"):
            res += textwrap.wrap(lines, width)
        return res
