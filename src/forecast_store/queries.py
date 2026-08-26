"""Canonical query builders (spec §9).

These return ``(sql, params)`` pairs for psycopg-style ``%s`` placeholders.
They are the SDK-side counterparts of the spec's canonical queries; keeping
them here (rather than hand-written at call sites) is what lets the skill,
the adapters, and user code share one tested query shape.
"""

from __future__ import annotations

from datetime import datetime

from forecast_store.config import StoreConfig


def forecast_asof(
    config: StoreConfig,
    series_name: str,
    target_start: datetime,
    target_end: datetime,
    asof: datetime,
) -> tuple[str, tuple]:
    """Latest vintage per target at or before ``asof`` (spec §9.1)."""
    s = config.schema
    cols = ", ".join(f"f.{c}" for c in config.value_columns)
    sql = f"""\
SELECT DISTINCT ON (f.series_id, f.target_time)
       f.series_id, f.target_time, f.available_at, f.run_id, {cols}
FROM {s}.forecasts f
WHERE f.series_id = {s}.get_series_id(%s)
  AND f.target_time >= %s AND f.target_time < %s
  AND f.available_at <= %s
ORDER BY f.series_id, f.target_time, f.available_at DESC"""
    return sql, (series_name, target_start, target_end, asof)
