"""The ``Store`` facade: one object bound to a connection and a declaration.

Sugar over the module functions, which stay the documented lower layer: a
Store holds the ``(conn, config)`` pair every SDK call takes and forwards to
:mod:`forecast_store.series`, :mod:`forecast_store.write`,
:mod:`forecast_store.read` and :mod:`forecast_store.queries`. It never
commits — the caller owns the transaction exactly as with the functions.

Two ways in:

- ``Store(conn, config=None, *, schema=None)`` binds a connection the caller
  manages (commit and rollback are theirs);
- ``with Store.connect(source, ...) as store:`` opens a connection for the
  block from ``source`` — a DSN string, or a pool with a ``.connection()``
  context manager (psycopg_pool's, duck-typed; no dependency) — and inherits
  that context's commit-on-exit / rollback-on-exception. The block is the
  unit of work; a pool checkout is one.

An omitted ``config`` is read from the store's own ``store_tables`` on first
use (``schema`` says where, default ``forecast``). Pooled hot paths should
load the declaration once (:meth:`StoreConfig.from_store`) and pass it in.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from datetime import datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from forecast_store.config import StoreConfig
from forecast_store.errors import InvalidDeclaration
from forecast_store.queries import ForecastsAsOf, forecast_asof_columns
from forecast_store.queries import forecast_asof as _forecast_asof_sql
from forecast_store.read import (
    ContextSeries,
    VersionedSeries,
    read_context_series,
    read_versioned_series,
)
from forecast_store.series import SeriesRef, get_series_id, register_series
from forecast_store.write import Point, write_actuals, write_forecast, write_predictors


class _Pool(Protocol):
    """What :meth:`Store.connect` needs from a pool — psycopg_pool's shape."""

    def connection(self) -> AbstractContextManager[Any]: ...


#: A DSN string, or a pool whose ``.connection()`` is a context manager.
ConnectionSource = str | _Pool


def _schema_for(config: StoreConfig | None, schema: str | None) -> str:
    """The schema a store-bound object works in; a contradicting pair is a caller bug."""
    if config is not None and schema is not None and schema != config.schema:
        raise InvalidDeclaration(
            f"schema {schema!r} conflicts with config.schema {config.schema!r}"
        )
    return config.schema if config is not None else (schema or "forecast")


@contextmanager
def _connection(source: ConnectionSource) -> Iterator[Any]:
    """A connection for one block: opened from a DSN, or checked out of a pool.

    Either way the context commits on normal exit and rolls back on
    exception — psycopg's and psycopg_pool's own contract, inherited.
    """
    import psycopg

    if isinstance(source, str):
        with psycopg.connect(source) as conn:
            yield conn
    elif isinstance(source, psycopg.Connection):
        raise TypeError(
            "got an open connection: bind it with Store(conn, ...) — "
            "Store.connect() takes a DSN or a pool"
        )
    elif callable(getattr(source, "connection", None)):
        with source.connection() as conn:
            yield conn
    else:
        raise TypeError(
            "source must be a DSN string or a pool with a .connection() context "
            f"manager, got {type(source).__name__}"
        )


class Store:
    """A forecast store bound to one connection and one declaration.

    Every method forwards to the module function of the same name with
    ``(self.conn, self.config)`` prepended — see those for the full contracts.
    ``conn`` and ``schema`` are plain attributes; :attr:`config` is given, or
    read from the store on first use. Nothing here commits.
    """

    def __init__(
        self, conn: Any, config: StoreConfig | None = None, *, schema: str | None = None
    ) -> None:
        self.conn = conn
        self.schema = _schema_for(config, schema)
        self._config = config

    @classmethod
    @contextmanager
    def connect(
        cls,
        source: ConnectionSource,
        config: StoreConfig | None = None,
        *,
        schema: str | None = None,
    ) -> Iterator[Store]:
        """A Store for one block, on a connection opened from ``source``.

        ``source`` is a DSN, or a pool (anything with a ``.connection()``
        context manager). Leaving the block commits; an exception rolls back —
        psycopg's connection-context contract, inherited unchanged, so the
        block is the unit of work. Bind a connection you already hold with
        ``Store(conn, ...)`` instead.
        """
        _schema_for(config, schema)  # a contradicting pair fails before connecting
        with _connection(source) as conn:
            yield cls(conn, config, schema=schema)

    @property
    def config(self) -> StoreConfig:
        """The declaration — as given, or read from the store on first use."""
        if self._config is None:
            self._config = StoreConfig.from_store(self.conn, self.schema)
        return self._config

    # -- series registry ---------------------------------------------------

    def register_series(
        self,
        name: str,
        sample_interval: timedelta | str,
        *,
        timezone: str | None = None,
        unit: str | None = None,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> int:
        """Get-or-create a series; see :func:`forecast_store.series.register_series`."""
        return register_series(
            self.conn, self.config, name, sample_interval,
            timezone=timezone, unit=unit, description=description, metadata=metadata,
        )

    def get_series_id(self, name: str) -> int:
        """Strict resolver; see :func:`forecast_store.series.get_series_id`."""
        return get_series_id(self.conn, self.config, name)

    # -- writes ------------------------------------------------------------

    def write_forecast(
        self,
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
        params: Mapping[str, Any] | None = None,
    ) -> UUID:
        """One forecast (a run + its points); see :func:`forecast_store.write.write_forecast`."""
        return write_forecast(
            self.conn, self.config,
            series=series, model=model, points=points, available_at=available_at,
            table=table, model_version=model_version, run_name=run_name,
            context_start=context_start, context_end=context_end, params=params,
        )

    def write_actuals(
        self,
        series: SeriesRef,
        points: Iterable[Point],
        *,
        available_at: datetime | None = None,
        table: str = "actuals",
    ) -> None:
        """Observations; see :func:`forecast_store.write.write_actuals`."""
        write_actuals(
            self.conn, self.config, series, points, available_at=available_at, table=table
        )

    def write_predictors(
        self,
        series: SeriesRef,
        points: Iterable[Point],
        *,
        available_at: datetime | None = None,
        table: str = "predictors",
    ) -> None:
        """Vendor vintages; see :func:`forecast_store.write.write_predictors`."""
        write_predictors(
            self.conn, self.config, series, points, available_at=available_at, table=table
        )

    # -- reads -------------------------------------------------------------

    def read_context_series(
        self,
        series: SeriesRef,
        *,
        table: str,
        start: datetime,
        end: datetime,
        asof: datetime,
        column: str = "value",
        recorded_before: datetime | None = None,
        run_name: str | None = None,
    ) -> ContextSeries:
        """Leakage-free window; see :func:`forecast_store.read.read_context_series`."""
        return read_context_series(
            self.conn, self.config, series,
            table=table, start=start, end=end, asof=asof, column=column,
            recorded_before=recorded_before, run_name=run_name,
        )

    def read_versioned_series(
        self,
        series: SeriesRef,
        *,
        table: str,
        start: datetime,
        end: datetime,
        column: str = "value",
        recorded_before: datetime | None = None,
    ) -> VersionedSeries:
        """Belief-log export; see :func:`forecast_store.read.read_versioned_series`."""
        return read_versioned_series(
            self.conn, self.config, series,
            table=table, start=start, end=end, column=column, recorded_before=recorded_before,
        )

    def forecast_asof(
        self,
        series: SeriesRef,
        target_start: datetime,
        target_end: datetime,
        asof: datetime,
        *,
        table: str = "forecasts",
        recorded_before: datetime | None = None,
        run_name: str | None = None,
    ) -> ForecastsAsOf:
        """Latest vintage per target at or before ``asof`` (spec §9.1), executed.

        Parameters as :func:`forecast_store.queries.forecast_asof` — the
        ``(sql, params)`` builder, which stays available for hand-written
        SQL. Returns a :class:`ForecastsAsOf`: ``(columns, rows)`` plus
        ``.to_pandas()``.
        """
        sql, params = _forecast_asof_sql(
            self.config, series, target_start, target_end, asof,
            table=table, recorded_before=recorded_before, run_name=run_name,
        )
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return ForecastsAsOf(forecast_asof_columns(self.config, table), cur.fetchall())
