import argparse
from typing import Sequence


class StoreDictKeyValue(argparse.Action):
    """Custom action for argparse that will parse a list of key=value string and build a dict out of it.

    Example:

    >>> import argparse
    >>> parser = argparse.ArgumentParser()
    >>> parser.add_argument(
    ...     '--vars',
    ...     nargs='*',
    ...     dest='vars',
    ...     metavar='KEY=VALUE',
    ...     required=False,
    ...     action=StoreDictKeyValue,
    ...     help='Set a variable value. Can be repeated to define multiple variables.'
    ... ) # doctest:+ELLIPSIS
    StoreDictKeyValue(...)

    (ELLIPSIS tells doctest to match "..." with anything)

    >>> args = parser.parse_args(["--vars", "A=1"])
    >>> args.vars
    {'A': '1'}
    >>> args = parser.parse_args(["--vars", "B=2"])
    >>> args.vars
    {'B': '2'}

    """

    def __init__(self, option_strings: Sequence[str], dest: str, **kwargs) -> None:
        super().__init__(option_strings, dest, **kwargs)
        if self.default is not None:
            raise ValueError("No default value is allowed for StoreDictKeyValue action")
        self.default = dict()

    def __call__(self, parser, namespace, values, option_string=None) -> None:
        dest = dict()
        for value in values:
            k, v = value.split("=")
            dest[k] = v
        setattr(namespace, self.dest, dest)
