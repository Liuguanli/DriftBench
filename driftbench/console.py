from __future__ import annotations

import sys
from typing import TextIO


def console_print(
    *values: object,
    sep: str = " ",
    end: str = "\n",
    file: TextIO | None = None,
    flush: bool = False,
) -> None:
    """Print without failing when the active console cannot encode Unicode.

    Windows shells commonly use legacy encodings such as CP1252.  Converting
    unrepresentable characters to Python-style backslash escapes keeps both
    human-readable messages and serialized JSON valid on those consoles.
    Artifact files are written separately and remain UTF-8.
    """

    stream = file if file is not None else sys.stdout
    message = sep.join(str(value) for value in values) + end
    encoding = getattr(stream, "encoding", None)
    if encoding:
        message = message.encode(encoding, errors="backslashreplace").decode(encoding)

    try:
        stream.write(message)
    except UnicodeEncodeError:
        # Defensive fallback for custom streams whose declared encoding does
        # not match the encoder used by write().  ASCII is universally safe.
        stream.write(message.encode("ascii", errors="backslashreplace").decode("ascii"))
    if flush:
        stream.flush()
