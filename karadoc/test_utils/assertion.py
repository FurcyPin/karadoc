from typing import Any


class AnythingClass:
    """Class that will always return true when compared to something.
    Useful for asserting functions calls with the `unittest.mock` library

    WARNING: that 'trick' may not work when compared to any object `obj` such that obj.__eq__(Anything) returns False.
    Objects with a well-implemented equality should return NotImplemented when compared to something
    they don't know, not False.

    - Anything == Other will always return True
    - Other == Anything will return Other.__eq__(Anything) if is implemented, True otherwise

    Example:

    >>> AnythingClass() == ""
    True
    >>> "" == AnythingClass()
    True
    >>> "".__eq__(AnythingClass())
    NotImplemented

    >>> class NothingClass:
    ...    def __eq__(self, other):
    ...        return False
    >>> AnythingClass() == NothingClass()
    True
    >>> NothingClass() == AnythingClass()
    False
    >>> NothingClass().__eq__(AnythingClass())
    False
    """

    def __eq__(self, other: Any) -> bool:
        return True

    def __hash__(self) -> int:
        return 0


Anything = AnythingClass()
"""Static object that will always return true when compared to something.
Useful for asserting functions calls with the `unittest.mock` library.

WARNING: that 'trick' may not work when compared to any object `obj` such that obj.__eq__(Anything) returns False.
Objects with a well-implemented equality should return NotImplemented when compared to something
they don't know, not False.
See the examples in `AnythingClass` for more details.
"""
