"""The forecast write path: one run + its points, one transaction.

Callers own the transaction (spec/skill rule: a run row and its points commit
together, so the frozen-backtest pin can never split a run in half). This
function never commits.

Every write validates ``target_time`` against the registry's declared
``sample_interval`` (spec §5.1: the shared bucket grid is enforced at the SDK
write path; the generated ``data_quality_sweep`` is the backstop for raw-SQL
writers). Vintage times (``available_at``) are not grid-bound.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from forecast_store.config import StoreConfig

Point = tuple[datetime, Mapping[str, float | None]]

#: time_bucket's default origin (2000-01-03, a Monday); sub-day intervals that
#: divide a day are origin-insensitive, this keeps the rest exact.
_GRID_ORIGIN = datetime(2000, 1, 3, tzinfo=timezone.utc).timestamp()
_UNCHECKABLE_SECONDS = 28 * 86400  # month-sized intervals have no fixed stride


class MisalignedTimestamp(ValueError):
    """A target_time off the series' declared bucket grid (spec §4.1)."""


class ConflictingBelief(ValueError):
    """A single-belief table already holds a different value for this target
    (spec §6.1). Raised by the generated ``belief_guard`` trigger; identical
    re-delivery is silently idempotent instead."""


def _check_grid(cur: Any, schema: str, series_id: int, timestamps) -> None:
    from forecast_store.read import UnknownSeries

    cur.execute(
        f"SELECT extract(epoch FROM sample_interval) FROM {schema}.series "
        "WHERE series_id = %s",
        (series_id,),
    )
    row = cur.fetchone()
    if row is None:
        raise UnknownSeries(f"series_id {series_id} is not registered")
    secs = float(row[0])
    if secs <= 0 or secs >= _UNCHECKABLE_SECONDS:
        return
    for ts in timestamps:
        rem = (ts.timestamp() - _GRID_ORIGIN) % secs
        if min(rem, secs - rem) > 1e-6:
            raise MisalignedTimestamp(
                f"target_time {ts.isoformat()} is off the declared "
                f"{secs:.0f}s grid of series {series_id} (spec §4.1)"
            )


def write_forecast_run(
    conn: Any,
    config: StoreConfig,
    *,
    series_id: int,
    model: str,
    points: Sequence[Point],
    available_at: datetime,
    table: str = "forecasts",
    model_version: str | None = None,
    run_name: str | None = None,
    context_start: datetime | None = None,
    context_end: datetime | None = None,
    params: Mapping[str, Any] | None = None,
):
    """Insert a run and its forecast points; returns the run_id.

    ``table`` names the forecast-log instance (validated against the store's
    own ``store_tables`` declaration — it must exist and carry run
    provenance); ``runs`` is shared across instances. ``points`` maps declared
    value-column names (``mean``, ``q50``, ...) to values; undeclared columns
    are rejected, absent ones stay NULL. ``recorded_at`` is never written — it
    is system time (spec §4.1).
    """
    from psycopg.types.json import Jsonb

    from forecast_store.read import _table_declaration

    s = config.schema
    used_cols: set[str] = set()
    for _, values in points:
        used_cols.update(values)

    with conn.cursor() as cur:
        declaration = _table_declaration(cur, s, table)
        if not declaration.get("has_runs"):
            raise ValueError(f"{table!r} is not a forecast log (no run provenance)")
        declared = declaration["value_columns"]
        unknown = used_cols - set(declared)
        if unknown:
            raise ValueError(
                f"columns {sorted(unknown)} are not declared by {table!r} "
                f"(value columns: {declared})"
            )
        _check_grid(cur, s, series_id, (ts for ts, _ in points))
        cols = [c for c in declared if c in used_cols]
        cur.execute(
            f"INSERT INTO {s}.runs "
            "(run_name, model, model_version, available_at, context_start, context_end, params) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING run_id",
            (
                run_name,
                model,
                model_version,
                available_at,
                context_start,
                context_end,
                Jsonb(dict(params)) if params is not None else None,
            ),
        )
        run_id = cur.fetchone()[0]

        col_sql = "".join(f", {c}" for c in cols)
        placeholders = ", ".join(["%s"] * (4 + len(cols)))
        cur.executemany(
            f"INSERT INTO {s}.{table} "
            f"(run_id, series_id, target_time, available_at{col_sql}) "
            f"VALUES ({placeholders})",
            [
                (run_id, series_id, ts, available_at, *[values.get(c) for c in cols])
                for ts, values in points
            ],
        )
    return run_id


def write_actuals(
    conn: Any,
    config: StoreConfig,
    series_id: int,
    points: Sequence[tuple],
    *,
    available_at: datetime | None = None,
    table: str = "actuals",
) -> None:
    """Ingest observations. Idempotent (``ON CONFLICT DO NOTHING``).

    ``table`` names an actuals-role instance; its stored declaration supplies
    the tier and optional columns. Points are ``(target_time, value)`` — or
    ``(target_time, value, target_time_observed)`` on instances that declare
    the observed column (spec §6.1; the third element may be None).

    ``available_at=None`` measures arrival (column default); an explicit
    value is the backfill path — a written claim (spec §6.1), legal on both
    shapes (a single-belief backfill may state genuine historical arrival).

    Idempotency follows the PK switch (spec §6.1): on single-belief instances
    an identical re-delivery is a silent no-op and a *conflicting* value
    raises :class:`ConflictingBelief` — never silently swallowed. On
    revisioned instances the PK names a belief's full coordinates, so a
    colliding row is a *retcon* and is refused first-wins
    (``ON CONFLICT DO NOTHING``); genuine corrections arrive as new rows
    under a new ``available_at``.
    """
    import psycopg

    from forecast_store.read import _table_declaration

    s = config.schema
    with conn.cursor() as cur:
        declaration = _table_declaration(cur, s, table)
        if declaration.get("role") != "actuals":
            raise ValueError(f"{table!r} is not an actuals instance")
        revisions = declaration.get("revisions", True)
        has_observed = bool(declaration.get("has_target_time_observed"))
        if any(len(p) > 2 for p in points) and not has_observed:
            raise ValueError(
                f"{table!r} does not declare target_time_observed (spec §6.1)"
            )
        _check_grid(cur, s, series_id, (p[0] for p in points))

        cols = ["series_id", "target_time"]
        row_tail: list[Any] = []
        if has_observed:
            cols.append("target_time_observed")
        if available_at is not None:
            cols.append("available_at")
            row_tail = [available_at]
        cols.append("value")

        def row(p: tuple) -> tuple:
            ts, value = p[0], p[1]
            observed = ([p[2] if len(p) > 2 else None]) if has_observed else []
            return (series_id, ts, *observed, *row_tail, value)

        if revisions:
            conflict = "ON CONFLICT DO NOTHING"
        else:
            # SET touches only value: the stored claim and recorded_at are
            # preserved (first claim wins); the belief_guard trigger turns
            # the update into skip-or-raise.
            conflict = ("ON CONFLICT (series_id, target_time) "
                        "DO UPDATE SET value = EXCLUDED.value")
        placeholders = ", ".join(["%s"] * len(cols))
        try:
            cur.executemany(
                f"INSERT INTO {s}.{table} ({', '.join(cols)}) "
                f"VALUES ({placeholders}) {conflict}",
                [row(p) for p in points],
            )
        except psycopg.errors.IntegrityConstraintViolation as e:
            raise ConflictingBelief(str(e).splitlines()[0]) from e


def write_predictors(
    conn: Any,
    config: StoreConfig,
    series_id: int,
    points: Sequence[tuple[datetime, datetime, float | None]],
) -> None:
    """Ingest external forecast vintages as ``(target_time, available_at, value)``.

    ``available_at`` is the vendor publication time — the natural vintage key
    (spec §6.2). ``value`` is the vendor's point value; its statistic
    (deterministic run, ensemble mean, median) is per-feed registry metadata,
    never a column-name claim. Idempotent.
    """
    s = config.schema
    with conn.cursor() as cur:
        _check_grid(cur, s, series_id, (ts for ts, _, _ in points))
        cur.executemany(
            f"INSERT INTO {s}.predictors (series_id, target_time, available_at, value) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            [(series_id, ts, avail, v) for ts, avail, v in points],
        )
