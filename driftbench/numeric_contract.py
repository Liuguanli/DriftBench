"""Shared numeric validation and human-facing presentation rules.

Computational values must keep their full precision.  The presentation helpers
in this module return bounded-precision copies for notebooks, console text, and
other human-facing summaries; they must not feed sampling, statistics, hashes,
or canonical machine artifacts.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from decimal import Decimal, ROUND_FLOOR
from numbers import Integral, Real
from typing import Any


DISPLAY_DECIMAL_PLACES = 6
MIN_INTEGER = -(1 << 63)
MAX_INTEGER = (1 << 63) - 1

_PROBABILITY_MAP_FIELDS = frozenset(
    {
        "baseline_weights",
        "frequencies",
        "probabilities",
        "target_weights",
        "weights",
    }
)


def require_integer(
    value: Any,
    field: str,
    *,
    minimum: int = MIN_INTEGER,
    maximum: int = MAX_INTEGER,
) -> int:
    """Return a strict Python integer within an explicit semantic range."""

    if type(value) is not int:
        raise TypeError(f"{field} must be an integer")
    if value < minimum or value > maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return value


def require_real(
    value: Any,
    field: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    """Return a finite real number within optional inclusive bounds."""

    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError(f"{field} must be a real number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field} must be finite")
    if minimum is not None and number < minimum:
        raise ValueError(f"{field} must be >= {minimum}")
    if maximum is not None and number > maximum:
        raise ValueError(f"{field} must be <= {maximum}")
    return number


def require_probability(value: Any, field: str) -> float:
    """Return a finite ratio in the closed unit interval."""

    return require_real(value, field, minimum=0.0, maximum=1.0)


def present_real(
    value: Any, *, decimal_places: int = DISPLAY_DECIMAL_PLACES
) -> float:
    """Return a finite float suitable for human-facing structured output.

    Values use at most ``decimal_places`` fractional digits.  A tiny non-zero
    value that would round to zero is retained with the same number of
    significant digits so Python/JSON can display it in compact scientific
    notation.  Integral continuous values remain floats by design.
    """

    if type(decimal_places) is not int or not 0 <= decimal_places <= 15:
        raise ValueError("decimal_places must be an integer between 0 and 15")
    number = require_real(value, "value")
    if number == 0:
        return 0.0
    rounded = round(number, decimal_places)
    if rounded == 0:
        return float(f"{number:.{max(1, decimal_places)}g}")
    return float(rounded)


def format_number(
    value: Any, *, decimal_places: int = DISPLAY_DECIMAL_PLACES
) -> str:
    """Format one finite number with the shared human-facing policy."""

    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError("value must be a real number")
    if isinstance(value, Integral) and not isinstance(value, bool):
        integer = require_integer(int(value), "value")
        return str(integer)
    displayed = present_real(value, decimal_places=decimal_places)
    if displayed == 0:
        return "0"
    quantum = 10.0 ** (-decimal_places)
    if abs(displayed) < quantum:
        return f"{displayed:.{max(1, decimal_places)}g}"
    return f"{displayed:.{decimal_places}f}".rstrip("0").rstrip(".")


def present_probability_map(
    values: Mapping[str, Real],
    *,
    field: str = "probabilities",
    decimal_places: int = DISPLAY_DECIMAL_PLACES,
) -> dict[str, float]:
    """Quantize a normalized probability map without changing its unit mass.

    The largest-remainder method prevents independent rounding from producing
    totals such as ``1.000001``.  Ties follow mapping insertion order, making
    presentation deterministic.  A positive probability smaller than one
    display quantum may appear as ``0.0`` so the displayed group retains exact
    unit mass.  This function is presentation-only.
    """

    if not isinstance(values, Mapping) or not values:
        raise ValueError(f"{field} must be a non-empty mapping")
    if type(decimal_places) is not int or not 0 <= decimal_places <= 15:
        raise ValueError("decimal_places must be an integer between 0 and 15")

    keys = list(values)
    if not all(isinstance(key, str) and key for key in keys):
        raise TypeError(f"{field} keys must be non-empty strings")
    probabilities = [
        require_probability(values[key], f"{field}[{key!r}]") for key in keys
    ]
    if not math.isclose(math.fsum(probabilities), 1.0, rel_tol=0.0, abs_tol=1e-12):
        raise ValueError(f"{field} must sum to 1")

    scale = 10**decimal_places
    exact_units = [Decimal(str(value)) * scale for value in probabilities]
    units = [
        int(value.to_integral_value(rounding=ROUND_FLOOR))
        for value in exact_units
    ]
    remaining = scale - sum(units)
    if remaining < 0 or remaining > len(units):
        raise ValueError(f"{field} cannot be represented at {decimal_places} decimal places")
    order = sorted(
        range(len(units)),
        key=lambda index: (-(exact_units[index] - units[index]), index),
    )
    for index in order[:remaining]:
        units[index] += 1

    if sum(units) != scale:
        raise AssertionError("display probability allocation lost unit mass")
    return {key: float(units[index] / scale) for index, key in enumerate(keys)}


def present_value(
    value: Any,
    *,
    field: str | None = None,
    decimal_places: int = DISPLAY_DECIMAL_PLACES,
) -> Any:
    """Return a recursively bounded-precision copy for human presentation.

    Integer and boolean types are preserved.  Known probability-map field
    names are quantized as a group; other floats are rounded independently.
    """

    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, Integral) and not isinstance(value, bool):
        return require_integer(int(value), field or "value")
    if isinstance(value, Real):
        return present_real(value, decimal_places=decimal_places)
    if isinstance(value, Mapping):
        if field in _PROBABILITY_MAP_FIELDS and value:
            candidates = tuple(value.values())
            if all(
                isinstance(item, Real)
                and not isinstance(item, bool)
                and math.isfinite(float(item))
                and 0.0 <= float(item) <= 1.0
                for item in candidates
            ) and math.isclose(
                math.fsum(float(item) for item in candidates),
                1.0,
                rel_tol=0.0,
                abs_tol=1e-12,
            ):
                return present_probability_map(
                    value, field=field, decimal_places=decimal_places
                )
        return {
            key: present_value(item, field=str(key), decimal_places=decimal_places)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            present_value(item, decimal_places=decimal_places) for item in value
        ]
    if isinstance(value, tuple):
        return tuple(
            present_value(item, decimal_places=decimal_places) for item in value
        )
    return value


__all__ = [
    "DISPLAY_DECIMAL_PLACES",
    "MAX_INTEGER",
    "MIN_INTEGER",
    "format_number",
    "present_probability_map",
    "present_real",
    "present_value",
    "require_integer",
    "require_probability",
    "require_real",
]
