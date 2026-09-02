"""Timestamp validation shared by the read and write paths.

The store speaks ``timestamptz``. A naive ``datetime`` handed to psycopg is
cast in the *session* time zone, while ``datetime.timestamp()`` reads it in
the *process's* local zone — two silent, possibly different,
reinterpretations of the same value. So every timestamp that reaches SQL
must be aware: :func:`aware` enforces it before any statement runs, and
:func:`check_grid` validates target times against the series' declared
bucket grid (spec §4.1, §5.1).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from forecast_store.errors import MisalignedTimestamp, NaiveTimestamp

#: time_bucket's default origin (2000-01-03, a Monday); sub-day intervals that
#: divide a day are origin-insensitive, this keeps the rest exact.
_GRID_ORIGIN = datetime(2000, 1, 3, tzinfo=timezone.utc).timestamp()
_UNCHECKABLE_SECONDS = 28 * 86400  # month-sized intervals have no fixed stride


def aware(value: Any, name: str) -> datetime:
    """``value`` itself, if it is a timezone-aware ``datetime``.

    Raises ``TypeError`` for anything that is not a datetime and
    :class:`NaiveTimestamp` for one without a usable ``tzinfo``.
    """
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime, got {type(value).__name__}")
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise NaiveTimestamp(
            f"{name} {value.isoformat()} is naive (no tzinfo): the store speaks "
            "timestamptz, and a naive value would be reinterpreted in the session "
            "time zone — pass an aware datetime, e.g. tzinfo=timezone.utc"
        )
    return value


def check_grid(interval: timedelta, series: Any, timestamps) -> None:
    """Every target time sits on the series' declared bucket grid (spec §4.1)."""
    secs = interval.total_seconds()
    if secs <= 0 or secs >= _UNCHECKABLE_SECONDS:
        return
    for ts in timestamps:
        rem = (ts.timestamp() - _GRID_ORIGIN) % secs
        if min(rem, secs - rem) > 1e-6:
            raise MisalignedTimestamp(
                f"target_time {ts.isoformat()} is off the declared "
                f"{secs:.0f}s grid of series {series!r} (spec §4.1)"
            )
