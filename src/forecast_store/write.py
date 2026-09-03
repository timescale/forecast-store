"""The write path: points onto the declared grid, one transaction.

Callers own the transaction (spec/skill rule: a run row and its points commit
together, so the frozen-backtest pin can never split a run in half). Nothing
here commits.

One point shape for every table: ``(target_time, values)``, where ``values``
maps declared column names to values — ``mean``/``q50``/... on a forecast
log, ``value`` (plus a band, if declared) on actuals and predictors — and may
carry the knowledge column ``available_at`` and, where declared,
``target_time_observed``. A bare scalar is shorthand for the table's single
value column. The table's stored declaration decides what is writable and
how the knowledge time resolves:

- forecast logs: ``available_at`` is the run's; a per-point key is rejected
  (one run, one knowledge time — spec §4.1);
- actuals: per-point key, else the call-level ``available_at``, else the
  column default — arrival is *measured* (spec §6.1);
- predictors: per-point key, else the call-level ``available_at``, else an
  error — publication is a *claim* and must be stated (spec §6.2).

Every write takes a *series reference* — the registered name or the
``series_id`` (see :mod:`forecast_store.series`) — and validates
``target_time`` against the registry's declared ``sample_interval`` (spec
§5.1: the shared bucket grid is enforced at the SDK write path; the generated
``data_quality_sweep`` is the backstop for raw-SQL writers). Knowledge times
are not grid-bound. Every timestamp must be timezone-aware
(:class:`~forecast_store.errors.NaiveTimestamp` otherwise), checked before
any statement runs.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

from forecast_store.config import StoreConfig
from forecast_store.errors import ConflictingBelief, DeclarationMismatch, MisalignedTimestamp
from forecast_store.read import _table_declaration
from forecast_store.series import SeriesRef, _lookup
from forecast_store.timestamps import aware, check_grid

__all__ = [
    "ConflictingBelief",  # errors re-exported for the import paths writers already use
    "MisalignedTimestamp",
    "Point",
    "write_actuals",
    "write_forecast",
    "write_predictors",
]

#: ``(target_time, values)``: ``values`` maps declared columns to values, or is
#: a bare scalar for a table with exactly one value column.
Point = tuple[datetime, Mapping[str, Any] | float | None]

_OBSERVED = "target_time_observed"


def _normalize(
    table: str,
    declaration: Mapping[str, Any],
    points: Iterable[Point],
    *,
    per_point_knowledge: bool,
) -> tuple[list[str], list[tuple[datetime, dict[str, Any]]]]:
    """Validate points against the table's stored declaration.

    Returns the writable columns actually used (declaration order) and the
    points with scalars expanded to mappings. Nothing has touched the
    database when this raises. ``per_point_knowledge`` says whether the
    knowledge column is writable per point (actuals, predictors) or fixed by
    the run (forecast logs).
    """
    value_columns = list(declaration["value_columns"])
    knowledge_col = declaration["knowledge_column"]
    writable = list(value_columns)
    if per_point_knowledge:
        writable.append(knowledge_col)
    if declaration.get("has_target_time_observed"):
        writable.append(_OBSERVED)

    rows: list[tuple[datetime, dict[str, Any]]] = []
    used: set[str] = set()
    for ts, values in points:
        aware(ts, "target_time")
        if not isinstance(values, Mapping):
            if len(value_columns) != 1:
                raise DeclarationMismatch(
                    f"{table!r} declares {len(value_columns)} value columns "
                    f"{value_columns}: pass a mapping of column -> value, not a bare scalar"
                )
            values = {value_columns[0]: values}
        unknown = set(values) - set(writable)
        if unknown:
            if knowledge_col in unknown and not per_point_knowledge:
                raise DeclarationMismatch(
                    f"{table!r} is a forecast log: its knowledge time is the run's "
                    f"available_at, not a per-point column (spec §4.1)"
                )
            raise DeclarationMismatch(
                f"columns {sorted(unknown)} are not declared by {table!r} "
                f"(writable columns: {writable})"
            )
        for stamp in (knowledge_col, _OBSERVED):  # per-point timestamps, where given
            if values.get(stamp) is not None:
                aware(values[stamp], stamp)
        used.update(values)
        rows.append((ts, dict(values)))
    return [c for c in writable if c in used], rows


def _stated(values: Mapping[str, Any], column: str, default: datetime | None) -> datetime | None:
    """Per-point knowledge time, else the call-level default (None = unstated)."""
    stated = values.get(column)
    return default if stated is None else stated


def write_forecast(
    conn: Any,
    config: StoreConfig,
    *,
    series: SeriesRef,
    model: str,
    points: Iterable[Point],
    available_at: datetime,
    table: str = "forecasts",
    model_version: str | None = None,
    run_name: str | None = None,
    context_start: datetime | None = None,
    context_end: datetime | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    params: Mapping[str, Any] | None = None,
) -> UUID:
    """Write one forecast — a run row plus its points, one knowledge time; returns the run_id.

    ``series`` is the registered name or ``series_id``. ``table`` names the
    forecast-log instance (validated against the store's own ``store_tables``
    declaration — it must exist and carry run provenance); ``runs`` is shared
    across instances. Each point's values map declared value columns
    (``mean``, ``q50``, ...) to values — absent columns stay NULL, undeclared
    ones are rejected — and ``available_at`` is the run's knowledge time for
    every point (a per-point key is rejected). ``recorded_at`` is never
    written — it is system time (spec §4.1).
    """
    from psycopg.types.json import Jsonb

    aware(available_at, "available_at")
    for name, stamp in (
        ("context_start", context_start), ("context_end", context_end),
        ("started_at", started_at), ("finished_at", finished_at),
    ):
        if stamp is not None:
            aware(stamp, name)

    s = config.schema
    with conn.cursor() as cur:
        series_id, interval = _lookup(cur, s, series)
        declaration = _table_declaration(cur, s, table)
        if not declaration.get("has_runs"):
            raise DeclarationMismatch(f"{table!r} is not a forecast log (no run provenance)")
        cols, rows = _normalize(table, declaration, points, per_point_knowledge=False)
        check_grid(interval, series, (ts for ts, _ in rows))
        knowledge_col = declaration["knowledge_column"]

        cur.execute(
            f"INSERT INTO {s}.runs "
            "(run_name, model, model_version, available_at, context_start, context_end, "
            "started_at, finished_at, params) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING run_id",
            (
                run_name,
                model,
                model_version,
                available_at,
                context_start,
                context_end,
                started_at,
                finished_at,
                Jsonb(dict(params)) if params is not None else None,
            ),
        )
        run_id = cur.fetchone()[0]

        col_sql = "".join(f", {c}" for c in cols)
        placeholders = ", ".join(["%s"] * (4 + len(cols)))
        cur.executemany(
            f"INSERT INTO {s}.{table} "
            f"(run_id, series_id, target_time, {knowledge_col}{col_sql}) "
            f"VALUES ({placeholders})",
            [
                (run_id, series_id, ts, available_at, *[values.get(c) for c in cols])
                for ts, values in rows
            ],
        )
    return run_id


def write_actuals(
    conn: Any,
    config: StoreConfig,
    series: SeriesRef,
    points: Iterable[Point],
    *,
    available_at: datetime | None = None,
    table: str = "actuals",
) -> None:
    """Ingest observations. Idempotent (``ON CONFLICT DO NOTHING``).

    ``series`` is the registered name or ``series_id``; ``table`` names an
    actuals-role instance whose stored declaration supplies the tier and
    optional columns. Points are ``(target_time, value)`` or
    ``(target_time, {"value": v, ...})`` with an optional ``available_at``
    and — on instances that declare it (spec §6.1) — ``target_time_observed``.

    Knowledge time: a per-point ``available_at`` wins, else the call-level
    one, else the column default measures arrival. Any stated value is the
    backfill path — a written claim (spec §6.1), legal on both shapes (a
    single-belief backfill may state genuine historical arrival).

    Idempotency follows the PK switch (spec §6.1): on single-belief instances
    an identical re-delivery is a silent no-op and a *conflicting* value
    raises :class:`ConflictingBelief` — never silently swallowed. On
    revisioned instances the PK names a belief's full coordinates, so a
    colliding row is a *retcon* and is refused first-wins
    (``ON CONFLICT DO NOTHING``); genuine corrections arrive as new rows
    under a new ``available_at``.
    """
    import psycopg

    if available_at is not None:
        aware(available_at, "available_at")

    s = config.schema
    with conn.cursor() as cur:
        series_id, interval = _lookup(cur, s, series)
        declaration = _table_declaration(cur, s, table)
        if declaration.get("role") != "actuals":
            raise DeclarationMismatch(f"{table!r} is not an actuals instance")
        cols, rows = _normalize(table, declaration, points, per_point_knowledge=True)
        check_grid(interval, series, (ts for ts, _ in rows))
        if not rows:
            return
        knowledge_col = declaration["knowledge_column"]
        data_cols = [c for c in cols if c != knowledge_col]

        if declaration.get("revisions", True):
            conflict = "ON CONFLICT DO NOTHING"
        else:
            # SET touches only value: the stored claim and recorded_at are
            # preserved (first claim wins); the belief_guard trigger turns
            # the update into skip-or-raise.
            conflict = ("ON CONFLICT (series_id, target_time) "
                        "DO UPDATE SET value = EXCLUDED.value")
        col_sql = "".join(f", {c}" for c in data_cols)
        # coalesce: an unstated knowledge time takes the column default (arrival
        # measured) even when other rows of the same batch state one.
        values_sql = ", ".join(
            ["%s", "%s", "coalesce(%s::timestamptz, now())", *["%s"] * len(data_cols)]
        )
        try:
            cur.executemany(
                f"INSERT INTO {s}.{table} (series_id, target_time, {knowledge_col}{col_sql}) "
                f"VALUES ({values_sql}) {conflict}",
                [
                    (
                        series_id,
                        ts,
                        _stated(values, knowledge_col, available_at),
                        *[values.get(c) for c in data_cols],
                    )
                    for ts, values in rows
                ],
            )
        except psycopg.errors.IntegrityConstraintViolation as e:
            raise ConflictingBelief(str(e).splitlines()[0]) from e


def write_predictors(
    conn: Any,
    config: StoreConfig,
    series: SeriesRef,
    points: Iterable[Point],
    *,
    available_at: datetime | None = None,
    table: str = "predictors",
) -> None:
    """Ingest external forecast vintages. Idempotent (``ON CONFLICT DO NOTHING``).

    ``series`` is the registered name or ``series_id``; ``table`` names a
    predictors-role instance (spec §6.2) — the canonical feed table or a
    declared vendor instance, band and all. Points are
    ``(target_time, value)`` or ``(target_time, {"value": v, "q50": ..., ...})``
    with an optional per-point ``available_at``.

    Knowledge time is the vendor publication time — the natural vintage key
    (spec §6.2). A per-point ``available_at`` wins, else the call-level one
    (the common case: one vendor run, one publication, many targets); with
    neither the write is refused — publication is stated, never defaulted.
    The value's statistic (deterministic run, ensemble mean, median) is
    per-feed registry metadata, never a column-name claim.
    """
    if available_at is not None:
        aware(available_at, "available_at")

    s = config.schema
    with conn.cursor() as cur:
        series_id, interval = _lookup(cur, s, series)
        declaration = _table_declaration(cur, s, table)
        if declaration.get("role") != "predictors":
            raise DeclarationMismatch(f"{table!r} is not a predictors instance")
        cols, rows = _normalize(table, declaration, points, per_point_knowledge=True)
        check_grid(interval, series, (ts for ts, _ in rows))
        if not rows:
            return
        knowledge_col = declaration["knowledge_column"]
        data_cols = [c for c in cols if c != knowledge_col]
        stated = [_stated(values, knowledge_col, available_at) for _, values in rows]
        if any(k is None for k in stated):
            raise DeclarationMismatch(
                f"{table!r} needs a knowledge time on every point: pass available_at per "
                "point or for the call — vendor publication is stated, never defaulted "
                "(spec §6.2)"
            )

        col_sql = "".join(f", {c}" for c in data_cols)
        placeholders = ", ".join(["%s"] * (3 + len(data_cols)))
        cur.executemany(
            f"INSERT INTO {s}.{table} (series_id, target_time, {knowledge_col}{col_sql}) "
            f"VALUES ({placeholders}) ON CONFLICT DO NOTHING",
            [
                (series_id, ts, k, *[values.get(c) for c in data_cols])
                for (ts, values), k in zip(rows, stated)
            ],
        )
