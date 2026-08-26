"""OpenSTEF integration: the production write path (spec §10;
adapter reference: docs/integrations/openstef.md).

:class:`ForecastStoreCallback` implements openstef-models'
``ForecastingCallback``; on ``on_predict_end`` it stamps real knowledge time
and writes the run + points into a forecast store in one transaction. This is
the moment OpenSTEF itself does not record — production ``predict()`` output
carries no ``available_at`` (spec §10); the store is that recorder.

Mapping notes (docs/integrations/openstef.md):

- Quantile columns map through the bijective naming rules on both sides:
  ``quantile_P50`` -> ``q50``. The adapter validates the result's quantiles
  against the store's declared band and errors on a mismatch (connector
  policy: connectors meet the band; v0 does not interpolate).
- ``context_end`` is the last *observed target* timestamp, not the input's
  max index: OpenSTEF prediction inputs carry future covariate timestamps
  (weather beyond forecast_start), which are target times, not knowledge
  times — using them would false-positive the leakage audit. The method used
  is recorded in ``runs.params``.
- An optional ``stdev`` column in the result is not persisted (auxiliary
  statistic — closed item, spec §11); its presence is recorded in
  ``runs.params``.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg
from openstef_models.workflows.custom_forecasting_workflow import (
    CustomForecastingWorkflow,
    ForecastingCallback,
)

from dataclasses import dataclass

from forecast_store.config import StoreConfig
from forecast_store.naming import quantile_column
from forecast_store.read import read_context_series
from forecast_store.write import write_forecast_run


@dataclass(frozen=True)
class ForecastFeed:
    """A covariate served from a forecast log (spec §6.2): our own — or any
    provenance-bearing — forecast consumed as another model's input. Never
    copied into ``predictors``; read as-of with the same query shape.

    Args:
        series: The forecasted store series.
        column: Which declared value column to read (band column or ``mean``).
        run_name: Optionally pin the producing job; without it, the latest
            vintage across all producers wins.
        table: The forecast-log instance to read (multi-instance stores).
    """

    series: str
    column: str = "q50"
    run_name: str | None = None
    table: str = "forecasts"


@dataclass(frozen=True)
class Observed:
    """A covariate served from a series' *measurements* (``actuals``) rather
    than a vendor feed — e.g. a metered temperature used as a model input.
    Plain-string bindings default to the ``predictors`` table (the common
    case: covariates are usually external forecast feeds)."""

    series: str
    table: str = "actuals"


class StoreReader:
    """Store-backed context assembly for OpenSTEF workflows (spec §9.3).

    The store-served replacement for hand-rolled ``fetch_load_measurements`` /
    ``fetch_weather_forecast`` integrations: measured target history from
    ``actuals``, covariates from ``predictors`` selected **as-of the decision
    moment** — leakage-free by construction — on the registry-declared grid,
    gapfilled and locf-regularized.

    The returned dataset carries a ``store_context`` attribute (read
    provenance: ``covariates_asof``, gap statistics, sources) which
    :class:`ForecastStoreCallback` merges into ``runs.params`` — completing
    the leakage audit for both halves of the input (spec §9.3).
    """

    def __init__(self, dsn: str, store_config: StoreConfig | None = None) -> None:
        self._dsn = dsn
        self._config = store_config or StoreConfig()

    def context(
        self,
        target_series: str,
        covariates: "dict[str, str | Observed | ForecastFeed]",
        *,
        history_start: datetime,
        asof: datetime,
        horizon_end: datetime,
        target_column: str = "load",
        recorded_before: datetime | None = None,
    ):
        """Assemble one model input frame.

        Args:
            target_series: Store series holding the target's actuals.
            covariates: Engine column name -> source binding (the rename map,
                spec §7.4). A plain name reads a vendor feed (``predictors``,
                the common case); :class:`Observed` reads a series'
                measurements; :class:`ForecastFeed` reads a forecast log.
            history_start: Start of the target-history window.
            asof: The decision moment — end of measured history, and the
                as-of cutoff for every covariate vintage.
            horizon_end: End of the covariate window (covariates extend past
                ``asof`` into the forecast horizon; their timestamps are
                target times, their knowledge is bounded by ``asof``).
            target_column: Engine's name for the target column.
            recorded_before: Optional frozen-read pin (spec §9.2).
        """
        import pandas as pd
        from openstef_core.datasets import TimeSeriesDataset

        gap_stats: dict[str, int] = {}
        columns: dict[str, Any] = {}
        intervals: dict[str, Any] = {}

        with psycopg.connect(self._dsn) as conn:
            interval, rows = read_context_series(
                conn,
                self._config,
                target_series,
                table="actuals",  # the target's measured history, by contract (§9.3)
                start=history_start,
                end=asof,
                asof=asof,  # knowledge cutoff on the target too: a historical
                # asof must not see later revisions (Tier 2) or later loads (Tier 1)
                recorded_before=recorded_before,
            )
            intervals[target_series] = interval
            columns[target_column] = pd.Series(
                {ts: value for ts, _, value in rows}, dtype=float
            )
            gap_stats[target_column] = sum(1 for _, raw, _ in rows if raw is None)

            sources: dict[str, Any] = {target_column: target_series}
            for column, spec in covariates.items():
                if isinstance(spec, ForecastFeed):
                    series_name = spec.series
                    extra = {
                        "table": spec.table,
                        "column": spec.column,
                        "run_name": spec.run_name,
                    }
                    sources[column] = {
                        "series": spec.series,
                        "table": spec.table,
                        "column": spec.column,
                        "run_name": spec.run_name,
                    }
                elif isinstance(spec, Observed):
                    series_name = spec.series
                    extra = {"table": spec.table}
                    sources[column] = {"series": spec.series, "table": spec.table}
                else:
                    # Plain name: a vendor feed, the common covariate case.
                    series_name, extra = spec, {"table": "predictors"}
                    sources[column] = {"series": spec, "table": "predictors"}
                interval, rows = read_context_series(
                    conn,
                    self._config,
                    series_name,
                    start=history_start,
                    end=horizon_end,
                    asof=asof,
                    recorded_before=recorded_before,
                    **extra,
                )
                intervals[series_name] = interval
                columns[column] = pd.Series(
                    {ts: value for ts, _, value in rows}, dtype=float
                )
                gap_stats[column] = sum(1 for _, raw, _ in rows if raw is None)

        if len(set(intervals.values())) > 1:
            raise ValueError(f"series are on different grids: {intervals}")

        frame = pd.DataFrame(columns).sort_index()
        dataset = TimeSeriesDataset(data=frame, sample_interval=interval)
        # Provenance rides the dataset object itself (a plain attribute — not
        # df.attrs, which pandas operations silently drop) and is harvested by
        # ForecastStoreCallback into runs.params.
        dataset.store_context = {
            "covariates_asof": asof.isoformat(),
            "recorded_before": recorded_before.isoformat() if recorded_before else None,
            "sources": sources,
            "gap_stats": gap_stats,
        }
        return dataset


class ForecastStoreCallback(ForecastingCallback):
    """Persist every prediction of a workflow into a forecast store.

    Args:
        dsn: Store connection string.
        series_name: Store series this workflow forecasts (the target).
        store_config: The store's declaration; must match the provisioned
            store (defaults to the reference instantiation).
        auto_register: Register the series on first write. Permitted because
            OpenSTEF datasets carry ``sample_interval`` explicitly (spec §8).
        model_version: Trained-artifact identity (e.g. MLflow model version).
    """

    def __init__(
        self,
        dsn: str,
        series_name: str,
        *,
        store_config: StoreConfig | None = None,
        auto_register: bool = True,
        model_version: str | None = None,
    ) -> None:
        self._dsn = dsn
        self._series_name = series_name
        self._config = store_config or StoreConfig()
        self._auto_register = auto_register
        self._model_version = model_version
        self.last_run_id = None  # set after each successful write

    def on_predict_end(self, context, data, result) -> None:  # noqa: ANN001
        available_at = datetime.now(timezone.utc)  # real knowledge time
        workflow: CustomForecastingWorkflow = context.workflow

        col_map = self._quantile_column_map(result)
        points = self._points(result, col_map)
        context_start, context_end, provenance = self._context_bounds(data, result)

        params: dict[str, Any] = {
            "engine": "openstef",
            "model_id": str(workflow.model_id),
            "run_name": workflow.run_name,
            "experiment_tags": dict(workflow.experiment_tags),
            "forecast_start": result.forecast_start.isoformat(),
            "sample_interval": str(result.sample_interval),
            "target_column": result.target_column,
            "quantile_columns": col_map,
            **provenance,
        }
        if result.standard_deviation_column in result.data.columns:
            params["stdev_column_present"] = True  # not persisted: open item, spec §11

        # Read provenance attached by StoreReader (covariate knowledge bound):
        # completes the leakage audit for the input's covariate half (spec §9.3).
        store_context = getattr(data, "store_context", None)
        if store_context is not None:
            params["covariates_asof"] = store_context.get("covariates_asof")
            params["context_provenance"] = store_context

        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                if self._auto_register:
                    cur.execute(
                        f"SELECT {self._config.schema}.register_series(%s, %s)",
                        (self._series_name, result.sample_interval),
                    )
                else:
                    cur.execute(
                        f"SELECT {self._config.schema}.get_series_id(%s)",
                        (self._series_name,),
                    )
                series_id = cur.fetchone()[0]

            self.last_run_id = write_forecast_run(
                conn,
                self._config,
                series_id=series_id,
                model=self._model_name(workflow),
                model_version=self._model_version,
                run_name=workflow.run_name or str(workflow.model_id),
                available_at=available_at,
                context_start=context_start,
                context_end=context_end,
                params=params,
                points=points,
            )
            conn.commit()  # run + points: one transaction

    def _quantile_column_map(self, result) -> dict[str, str]:  # noqa: ANN001
        """OpenSTEF column name -> store column name, validated against the band."""
        col_map: dict[str, str] = {}
        for q in result.quantiles:
            level = Decimal(str(float(q)))
            if level not in self._config.quantile_band:
                raise ValueError(
                    f"forecast quantile {level} is not in the store's declared band "
                    f"{[str(b) for b in self._config.quantile_band]}; connectors must "
                    "meet the band (spec §7.3)"
                )
            col_map[q.format()] = quantile_column(level)
        return col_map

    @staticmethod
    def _points(result, col_map: dict[str, str]) -> list:  # noqa: ANN001
        points = []
        for ts, row in result.data.iterrows():
            values = {}
            for src, dst in col_map.items():
                v = row.get(src)
                values[dst] = None if v is None or (isinstance(v, float) and math.isnan(v)) else float(v)
            points.append((ts.to_pydatetime(), values))
        return points

    @staticmethod
    def _context_bounds(data, result):  # noqa: ANN001
        """Input window bounds + how context_end was derived (see module notes)."""
        frame = getattr(data, "data", None)
        if frame is None or not len(frame.index):
            return None, None, {"context_end_method": "unavailable"}
        context_start = frame.index.min().to_pydatetime()
        target = result.target_column
        last_observed = (
            frame[target].last_valid_index() if target in frame.columns else None
        )
        if last_observed is not None:
            return (
                context_start,
                last_observed.to_pydatetime(),
                {
                    "context_end_method": "last_observed_target",
                    "input_span": [
                        context_start.isoformat(),
                        frame.index.max().to_pydatetime().isoformat(),
                    ],
                },
            )
        return (
            context_start,
            frame.index.max().to_pydatetime(),
            {"context_end_method": "input_index_max"},
        )

    @staticmethod
    def _model_name(workflow: CustomForecastingWorkflow) -> str:
        forecaster = getattr(workflow.model, "forecaster", None)
        return type(forecaster).__name__ if forecaster is not None else type(workflow.model).__name__
