import sys
from contextlib import contextmanager
from io import StringIO
from typing import Generator, Tuple


@contextmanager
def captured_output() -> Generator[Tuple[StringIO, StringIO], None, None]:
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err
