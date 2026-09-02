"""Series registry access: register and resolve series (spec §5.1).

The registry is the store's ``series`` table. :func:`register_series` and
:func:`get_series_id` are the Python counterparts of the generated SQL
resolvers of the same names, so callers never hand-write the
schema-qualified call — and never hardcode the schema.

Every read and write in the SDK takes a *series reference*: the registered
name (``str``) or the ``series_id`` (``int``). Names are the friendlier form
— the store already holds the resolver — and ids stay accepted for callers
that hold one.

Like the write path, these functions never commit; the caller owns the
transaction.
"""

from __future__ import annotations

from datetime import timedelta
from numbers import Integral
from typing import Any, Mapping

from forecast_store.config import StoreConfig

#: A registered series name, or its ``series_id``.
SeriesRef = str | int


class UnknownSeries(Exception):
    """The series is not registered in the store."""


def register_series(
    conn: Any,
    config: StoreConfig,
    name: str,
    sample_interval: timedelta | str,
    *,
    timezone: str | None = None,
    unit: str | None = None,
    description: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> int:
    """Register a series (get-or-create); returns its ``series_id``.

    Wraps the generated ``register_series`` SQL function: an existing name
    returns its id unchanged — the stored ``sample_interval`` and metadata
    are *not* compared or updated (changing a series' grid is a migration,
    not a re-registration). ``sample_interval`` is a ``timedelta`` or an
    interval literal such as ``"15 minutes"``.
    """
    from psycopg.types.json import Jsonb

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {config.schema}.register_series(%s, %s::interval, %s, %s, %s, %s)",
            (
                name,
                sample_interval,
                timezone,
                unit,
                description,
                Jsonb(dict(metadata)) if metadata is not None else None,
            ),
        )
        return cur.fetchone()[0]


def get_series_id(conn: Any, config: StoreConfig, name: str) -> int:
    """Strict resolver: the ``series_id`` of a registered name.

    Raises :class:`UnknownSeries` for an unregistered name (the SQL
    counterpart raises a Postgres exception; this is its Python face).
    """
    with conn.cursor() as cur:
        series_id, _ = _lookup(cur, config.schema, name)
        return series_id


def _lookup(cur: Any, schema: str, series: SeriesRef) -> tuple[int, timedelta]:
    """Resolve a series reference to ``(series_id, sample_interval)``.

    Shared by the read and write paths: one registry round-trip yields both
    the id the tables key on and the grid the write path validates against.
    """
    if isinstance(series, str):
        column, key = "name", series
    elif isinstance(series, Integral) and not isinstance(series, bool):
        column, key = "series_id", int(series)
    else:
        raise TypeError(
            "series must be a registered name (str) or a series_id (int), "
            f"got {type(series).__name__}"
        )
    cur.execute(
        f"SELECT series_id, sample_interval FROM {schema}.series WHERE {column} = %s",
        (key,),
    )
    row = cur.fetchone()
    if row is None:
        raise UnknownSeries(f"{series!r} is not a registered series in {schema}.series")
    return row[0], row[1]
