import re


def strip_margin(text: str) -> str:
    """For every line in this string:
    Strip a leading prefix consisting of blanks or control characters followed by | from the line.

    This function is similar to Scala's stripMargin function.

    >>> print(strip_margin('''
    ...    |a
    ...    |b
    ...    |'''))
    a
    b
    <BLANKLINE>

    """
    s = re.sub(r"\n[ \t]*\|", "\n", text)
    if s.startswith("\n"):
        return s[1:]
    else:
        return s
