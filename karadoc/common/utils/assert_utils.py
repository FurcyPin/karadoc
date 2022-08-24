from typing import Any


def assert_true(assertion: bool, error_message: Any = None) -> None:
    """Raise an AssertionError with the given error_message if the assertion passed is false

    >>> assert_true(3==4, "3 <> 4")
    Traceback (most recent call last):
    ...
    AssertionError: 3 <> 4

    >>> assert_true(3==3, "3 <> 4")

    :param assertion: assertion that will be checked
    :param error_message: error message to display if the assertion is false
    """
    if not assertion:
        if error_message is None:
            raise AssertionError()
        else:
            raise AssertionError(str(error_message))
