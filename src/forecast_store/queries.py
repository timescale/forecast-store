"""Canonical query builders (spec §9).

These return ``(sql, params)`` pairs for psycopg-style ``%s`` placeholders.
They are the SDK-side counterparts of the spec's canonical queries; keeping
them here (rather than hand-written at call sites) is what lets the skill,
the adapters, and user code share one tested query shape. Builders touch no
database: a table's columns come from the ``StoreConfig`` declaration — the
same one the store persists (:func:`forecast_store.ddl.table_configs`).

The executed form lives on the facade: :meth:`forecast_store.Store.forecast_asof`
returns a :class:`ForecastsAsOf`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, NamedTuple

from forecast_store.config import StoreConfig
from forecast_store.ddl import table_configs
from forecast_store.errors import DeclarationMismatch, UnknownTable
from forecast_store.series import SeriesRef, _ref_column
from forecast_store.timestamps import aware

__all__ = ["ForecastsAsOf", "forecast_asof", "forecast_asof_columns"]

_KEY_COLUMNS = ("series_id", "target_time", "available_at", "run_id")


def _forecast_log(config: StoreConfig, table: str) -> dict[str, Any]:
    declaration = table_configs(config).get(table)
    if declaration is None:
        raise UnknownTable(
            f"{table!r} is not a table declared by this StoreConfig (schema {config.schema!r})"
        )
    if not declaration.get("has_runs"):
        raise DeclarationMismatch(
            f"{table!r} is not a forecast log (role {declaration.get('role')!r}, "
            "no run provenance)"
        )
    return declaration


def forecast_asof_columns(config: StoreConfig, table: str = "forecasts") -> tuple[str, ...]:
    """Column names of a :func:`forecast_asof` row, in order: the four key
    columns, then the table's declared value columns."""
    return _KEY_COLUMNS + tuple(_forecast_log(config, table)["value_columns"])


def forecast_asof(
    config: StoreConfig,
    series: SeriesRef,
    target_start: datetime,
    target_end: datetime,
    asof: datetime,
    *,
    table: str = "forecasts",
    recorded_before: datetime | None = None,
    run_name: str | None = None,
) -> tuple[str, tuple]:
    """Latest vintage per target at or before ``asof`` (spec §9.1), as ``(sql, params)``.

    ``series`` is the registered name — the SQL then calls the store's
    ``get_series_id`` resolver, so the statement reads the same when pasted
    into hand-written SQL — or the ``series_id``. ``table`` names the
    forecast-log instance; its value columns come from ``config``.
    ``recorded_before`` is the system-clock pin (spec §9.2). ``run_name``
    pins the producing job; otherwise the latest vintage across all
    producers wins. Rows are ``(series_id, target_time, available_at,
    run_id, *value_columns)`` — :func:`forecast_asof_columns` names them.
    """
    for name, stamp in (
        ("target_start", target_start), ("target_end", target_end), ("asof", asof)
    ):
        aware(stamp, name)
    if recorded_before is not None:
        aware(recorded_before, "recorded_before")
    declaration = _forecast_log(config, table)
    s = config.schema

    ref, key = _ref_column(series)
    series_predicate = (
        f"f.series_id = {s}.get_series_id(%s)" if ref == "name" else "f.series_id = %s"
    )
    predicates = [
        series_predicate,
        "f.target_time >= %s",
        "f.target_time < %s",
        f"f.{declaration['knowledge_column']} <= %s",
    ]
    params: list[Any] = [key, target_start, target_end, asof]
    join = ""
    if recorded_before is not None:
        predicates.append("f.recorded_at <= %s")
        params.append(recorded_before)
    if run_name is not None:
        join = f"\nJOIN {s}.runs r ON r.run_id = f.run_id"
        predicates.append("r.run_name = %s")
        params.append(run_name)

    cols = ", ".join(f"f.{c}" for c in declaration["value_columns"])
    where = "\n  AND ".join(predicates)
    sql = f"""\
SELECT DISTINCT ON (f.series_id, f.target_time)
       f.series_id, f.target_time, f.available_at, f.run_id, {cols}
FROM {s}.{table} f{join}
WHERE {where}
ORDER BY f.series_id, f.target_time, f.available_at DESC"""
    return sql, tuple(params)


class ForecastsAsOf(NamedTuple):
    """The executed :func:`forecast_asof`: one row per target — the latest
    vintage at or before ``asof``. Unpacks as ``(columns, rows)``;
    ``columns`` names the row positions."""

    columns: tuple[str, ...]
    rows: list[tuple]

    def to_pandas(self):
        """A ``DataFrame`` with :attr:`columns`; ``target_time`` and
        ``available_at`` as UTC."""
        from forecast_store.read import _pandas

        pd = _pandas()
        frame = pd.DataFrame(self.rows, columns=list(self.columns))
        for column in ("target_time", "available_at"):
            frame[column] = pd.to_datetime(frame[column], utc=True)
        return frame
