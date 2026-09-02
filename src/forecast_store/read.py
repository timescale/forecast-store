"""Context reads: regularized, leakage-free series windows (spec §9.3).

APIs take a **series reference** — the registered name or ``series_id``
(see :mod:`forecast_store.series`) — and **table names**. ``store_tables``
is the routing registry: each provisioned table declares its value columns,
knowledge clock, and whether it carries run provenance, and the reader
resolves everything from those declarations — no table semantics are
hardcoded here, which is also what makes additional instances (a second
forecast table) readable with zero new machinery. Table names are validated
against the registry before entering SQL, so they are whitelisted by
construction.

Reads are registry-driven on the grid too — the bucket grid comes from the
series' declared ``sample_interval``, never inferred from data — and use the
one-pass TimescaleDB form: ``locf(last(value, knowledge))`` inside
``time_bucket_gapfill``. Every read states its decision moment (``asof``) and
is leakage-free by construction: a belief claimed after that moment cannot be
returned (spec §9.1/§9.3).

Rows come back as ``(ts, raw_value, value)`` — ``raw_value`` is NULL for
buckets that had no data (feeding the gap budget); ``value`` is the
locf-filled series.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from forecast_store.config import StoreConfig
from forecast_store.series import SeriesRef, UnknownSeries, _lookup

__all__ = [
    "UnknownSeries",  # defined in forecast_store.series; re-exported here
    "UnknownTable",
    "read_context_series",
    "read_versioned_series",
]


class UnknownTable(Exception):
    """The table is not declared in store_tables."""


def _table_declaration(cur, schema: str, table: str) -> dict[str, Any]:
    """Resolve a table's declaration from store_tables (the routing registry)."""
    cur.execute(
        f"SELECT config FROM {schema}.store_tables WHERE table_name = %s", (table,)
    )
    row = cur.fetchone()
    if row is None:
        raise UnknownTable(f"{table!r} is not a table declared in {schema}.store_tables")
    declaration = row[0]
    if "value_columns" not in declaration:
        raise UnknownTable(
            f"{table!r} declares no value columns — not a readable points table "
            f"(role: {declaration.get('role')})"
        )
    return declaration


def _resolve(
    cur,
    config: StoreConfig,
    series: SeriesRef,
    table: str,
    column: str,
    run_name: str | None,
) -> tuple[int, timedelta, str]:
    """Shared validation; returns (series_id, sample_interval, knowledge_col)."""
    series_id, interval = _lookup(cur, config.schema, series)
    declaration = _table_declaration(cur, config.schema, table)
    if column not in declaration["value_columns"]:
        raise ValueError(
            f"{column!r} is not a declared value column of {table!r} "
            f"({declaration['value_columns']})"
        )
    if run_name is not None and not declaration.get("has_runs"):
        raise ValueError(f"run_name only applies to run-bearing tables, not {table!r}")
    return series_id, interval, declaration["knowledge_column"]


def read_context_series(
    conn: Any,
    config: StoreConfig,
    series: SeriesRef,
    *,
    table: str,
    start: datetime,
    end: datetime,
    asof: datetime,
    column: str = "value",
    recorded_before: datetime | None = None,
    run_name: str | None = None,
) -> tuple[timedelta, list[tuple[datetime, float | None, float | None]]]:
    """One regularized series window on the declared grid.

    ``series`` is the registered name or ``series_id``. ``table`` names the
    source (validated against ``store_tables``): the series' measurements
    (``actuals``), a vendor feed (``predictors``), or a forecast log
    (``forecasts`` — pick ``column`` from its declared band; ``run_name``
    optionally pins the producing job, otherwise the latest vintage across
    all producers wins).

    ``asof`` — the decision moment — is required on every read: a context read
    is a model input, and a model input always has one. Live reads pass now();
    an omitted cutoff would be a silent "latest belief, whenever it arrived."
    Truly cutoff-free reads (BI, exports) are not context reads and belong in
    plain SQL where the choice is visible.

    ``recorded_before`` is the system-clock pin (spec §9.2): pass it whenever
    the answer must be stable against future writes or exclude specific past
    ones — frozen backtests/evaluations, exact reproduction of a past read,
    snapshot consistency across a multi-read job, or pre-incident
    reconstruction. It stays optional because — unlike ``asof`` — its absence
    has an honest meaning: ``recorded_at`` is never client-written, so no row
    is future-dated and an unset pin is exactly "the store as it stands now."

    Returns ``(sample_interval, rows)``.
    """
    s = config.schema
    with conn.cursor() as cur:
        series_id, interval, knowledge_col = _resolve(
            cur, config, series, table, column, run_name
        )

        join = ""
        predicates = [
            "t.series_id = %s",
            "t.target_time >= %s",
            "t.target_time < %s",
            f"t.{knowledge_col} <= %s",
        ]
        params: list[Any] = [series_id, start, end, asof]
        if recorded_before is not None:
            predicates.append("t.recorded_at <= %s")
            params.append(recorded_before)
        if run_name is not None:
            join = f" JOIN {s}.runs r ON r.run_id = t.run_id"
            predicates.append("r.run_name = %s")
            params.append(run_name)

        cur.execute(
            f"""\
SELECT time_bucket_gapfill(%s::interval, t.target_time, %s, %s) AS ts,
       last(t.{column}, t.{knowledge_col})       AS raw_value,
       locf(last(t.{column}, t.{knowledge_col})) AS value
FROM {s}.{table} t{join}
WHERE {" AND ".join(predicates)}
GROUP BY 1
ORDER BY 1""",
            [interval, start, end, *params],
        )
        return interval, cur.fetchall()


def read_versioned_series(
    conn: Any,
    config: StoreConfig,
    series: SeriesRef,
    *,
    table: str,
    start: datetime,
    end: datetime,
    column: str = "value",
    recorded_before: datetime | None = None,
) -> tuple[timedelta, list[tuple[datetime, datetime, float | None]]]:
    """Belief-log export: the full vintage/revision history of one series.

    This is NOT a context read — it deliberately takes no ``asof``. It serves
    engines that apply their own point-in-time cutoffs downstream (OpenSTEF's
    backtest machinery consumes the whole ``(target_time, available_at)``
    history and enforces knowledge cutoffs per simulated event). The
    ``recorded_before`` pin still applies for frozen benchmarks (spec §9.2).

    The table's knowledge clock comes from its ``store_tables`` declaration
    (Tier-1 actuals export their measured ``recorded_at``, spec §6.1/§9.2).

    Returns ``(sample_interval, rows)`` with rows
    ``(target_time, available_at, value)`` ordered by target then knowledge.
    """
    s = config.schema
    with conn.cursor() as cur:
        series_id, interval, knowledge_col = _resolve(
            cur, config, series, table, column, run_name=None
        )

        predicates = ["series_id = %s", "target_time >= %s", "target_time < %s"]
        params: list[Any] = [series_id, start, end]
        if recorded_before is not None:
            predicates.append("recorded_at <= %s")
            params.append(recorded_before)

        cur.execute(
            f"""\
SELECT target_time, {knowledge_col} AS available_at, {column} AS value
FROM {s}.{table}
WHERE {" AND ".join(predicates)}
ORDER BY target_time, available_at""",
            params,
        )
        return interval, cur.fetchall()
