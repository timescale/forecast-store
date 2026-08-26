"""Canonical naming rules.

The convention's quantile-column rule (spec §7.3): column name is ``q`` +
the percent value, integer part zero-padded to two digits, decimal point
replaced by underscore. The rule is bijective so writers and readers can
round-trip levels and column names without a lookup table.

    0.05  -> q05
    0.5   -> q50
    0.025 -> q02_5
    0.999 -> q99_9
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

_COLUMN_RE = re.compile(r"^q(\d{2,3})(?:_(\d+))?$")


def _as_level(level: float | str | Decimal) -> Decimal:
    """Normalize a quantile level to an exact Decimal in (0, 1)."""
    try:
        # str() first so float inputs like 0.05 become Decimal('0.05'),
        # not the exact binary expansion of the float.
        d = level if isinstance(level, Decimal) else Decimal(str(level))
    except InvalidOperation as exc:
        raise ValueError(f"not a quantile level: {level!r}") from exc
    if not Decimal(0) < d < Decimal(1):
        raise ValueError(f"quantile level must be in (0, 1), got {level!r}")
    return d


def quantile_column(level: float | str | Decimal) -> str:
    """Return the canonical column name for a quantile level."""
    d = _as_level(level)
    pct = format((d * 100).normalize(), "f")
    int_part, _, frac_part = pct.partition(".")
    name = f"q{int_part.zfill(2)}"
    if frac_part:
        name += f"_{frac_part}"
    return name


def parse_quantile_column(name: str) -> Decimal:
    """Inverse of :func:`quantile_column`; raises ValueError on non-canonical names."""
    m = _COLUMN_RE.match(name)
    if not m:
        raise ValueError(f"not a quantile column name: {name!r}")
    int_part, frac_part = m.groups()
    pct = Decimal(f"{int_part}.{frac_part}" if frac_part else int_part)
    level = pct / 100
    # Reject non-canonical spellings ('q5', 'q050', 'q50_0'): the rule is
    # bijective only if exactly one spelling exists per level.
    if quantile_column(level) != name:
        raise ValueError(f"non-canonical quantile column name: {name!r}")
    return level
