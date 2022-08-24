import sys
import traceback
from typing import TextIO


def display_exception(err: BaseException, file: TextIO = sys.stderr) -> None:
    # This try-catch block is only here to print the error message with real newlines (rather than \n).
    print("Traceback (most recent call last):", file=file)
    traceback.print_tb(err.__traceback__, file=file)
    print(str(err).replace("\\n", "\n"), file=file)

    if err.__cause__:
        print("This exception was caused by another exception: ")
        display_exception(err.__cause__, file)
