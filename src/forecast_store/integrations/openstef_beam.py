"""openstef-beam integration: the backtest/evaluation harness (spec §10;
adapter reference: docs/integrations/openstef.md).

Two adapters, dropped into ``BenchmarkPipeline`` unmodified:

- :class:`TimescaleTargetProvider` — data in: versioned measurements from
  ``actuals``, versioned predictors from ``predictors``, as full belief-log
  exports (OpenSTEF's backtest engine applies its own knowledge cutoffs per
  simulated event — the store hands it the whole vintage history).
- :class:`TimescaleBenchmarkStorage` — results out: backtest predictions land
  in ``runs``/``forecasts`` with **simulated** ``available_at`` (writable
  knowledge time — the anti-SQL:2011 argument in running code, spec §4.3);
  evaluation reports land in ``evaluation_runs`` (faithful pydantic snapshot
  in ``params``) plus ``evaluation_series``/``evaluation_metrics`` (the
  queryable projection). Subset frames are **re-derived on load** from the
  stored backtest output + ground truth using the same operations the
  evaluation pipeline used — the spec §11 open-item experiment: params carry
  enough to reconstruct the report.

Run identity: everything a benchmark writes for one target carries
``run_name = f"{benchmark_run}/{target.name}"`` — the grouping label of
spec §7.1, shared between forecast runs and evaluation runs.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from openstef_beam.benchmarking.models import BenchmarkTarget
from openstef_beam.benchmarking.storage.base import BenchmarkStorage
from openstef_beam.benchmarking.storage.local_storage import LocalBenchmarkStorage
from openstef_beam.benchmarking.target_provider import TargetProvider
from openstef_beam.evaluation import EvaluationReport
from openstef_beam.evaluation.metric_providers import MetricProvider
from openstef_beam.evaluation.models import EvaluationSubsetReport, SubsetMetric
from openstef_beam.evaluation.models.window import Filtering
from openstef_core.datasets import (
    ForecastInputDataset,
    TimeSeriesDataset,
    VersionedTimeSeriesDataset,
)
from openstef_core.datasets.validated_datasets import ForecastDataset
from openstef_core.types import AvailableAt, Quantile
from pydantic import Field, PrivateAttr, TypeAdapter

from forecast_store.config import StoreConfig
from forecast_store.naming import parse_quantile_column, quantile_column
from forecast_store.read import _table_declaration
from forecast_store.store import ConnectionSource, Store, _schema_for

_METRICS_ADAPTER = TypeAdapter(list[SubsetMetric])
_FILTERING_ADAPTER = TypeAdapter(Filtering)


class TimescaleTargetProvider(TargetProvider[BenchmarkTarget, None]):
    """Serve benchmark targets and their data from a forecast store.

    Measurements and predictors come back as ``VersionedTimeSeriesDataset``
    belief-log exports on the registry-declared grid. ``recorded_before``
    optionally freezes the whole benchmark against later writes (spec §9.2).

    The store's declaration is read from its own ``store_tables`` on first
    use unless ``store_config`` is given; ``store_schema`` says where to look
    (default ``forecast``).
    """

    source: Any = Field(
        description="Store connection source: a DSN, or a pool with a .connection() "
        "context manager (one connection per call)."
    )
    targets: list[BenchmarkTarget]
    measurement_series: dict[str, str] = Field(
        description="target.name -> actuals series name"
    )
    predictor_series: dict[str, dict[str, str]] = Field(
        default_factory=dict,
        description="target.name -> {engine column -> predictor series name} (the rename map)",
    )
    metric_providers: list[MetricProvider] = Field(default_factory=list)
    store_config: StoreConfig | None = Field(
        default=None,
        description="The store's declaration; omitted = read from the store's own "
        "store_tables (at store_schema) on first use.",
    )
    store_schema: str | None = Field(
        default=None,
        description="Where the store lives when store_config is omitted (default 'forecast').",
    )
    data_margin: timedelta = Field(
        default=timedelta(days=2),
        description="Read window extends this far past benchmark_end (horizon tail).",
    )
    recorded_before: datetime | None = None

    _loaded: StoreConfig | None = PrivateAttr(default=None)

    def model_post_init(self, _context: Any) -> None:
        _schema_for(self.store_config, self.store_schema)  # a contradicting pair fails here

    @contextmanager
    def _store(self) -> Iterator[Store]:
        """A Store for one call; the declaration, once resolved, is kept."""
        with Store.connect(
            self.source, self.store_config or self._loaded, schema=self.store_schema
        ) as store:
            yield store
            self._loaded = store.config

    def _read_versioned(
        self, store: Store, series_name: str, target: BenchmarkTarget, column: str, table: str
    ):
        versioned = store.read_versioned_series(
            series_name,
            table=table,
            start=target.train_start,
            end=target.benchmark_end + self.data_margin,
            recorded_before=self.recorded_before,
        )
        frame = versioned.to_pandas().rename(columns={"target_time": "timestamp", "value": column})
        return VersionedTimeSeriesDataset.from_dataframe(frame, versioned.sample_interval)

    def get_targets(self, filter_args: None = None) -> list[BenchmarkTarget]:
        return list(self.targets)

    def get_measurements_for_target(self, target: BenchmarkTarget) -> VersionedTimeSeriesDataset:
        with self._store() as store:
            return self._read_versioned(
                store,
                self.measurement_series[target.name],
                target,
                self.target_column,
                table="actuals",
            )

    def get_predictors_for_target(self, target: BenchmarkTarget) -> VersionedTimeSeriesDataset:
        bindings = self.predictor_series.get(target.name, {})
        with self._store() as store:
            datasets = [
                self._read_versioned(store, series_name, target, column, table="predictors")
                for column, series_name in bindings.items()
            ]
        return VersionedTimeSeriesDataset.concat(datasets=datasets, mode="outer")

    def get_metrics_for_target(self, target: BenchmarkTarget) -> list[MetricProvider]:
        return list(self.metric_providers)

    def get_evaluation_mask_for_target(self, target: BenchmarkTarget):
        return None


def _noneify(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return float(value)


def _allow_workspace_decompression(cur) -> None:
    """Benchmark workspace ops (label-scoped overwrite, backfill-style writes)
    touch old target_time regions that the columnstore policy has compressed;
    lift TimescaleDB's DML decompression limit for this transaction only."""
    cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
    if cur.fetchone() is not None:
        cur.execute(
            "SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0"
        )


def _utc_normalize(frame, columns) -> None:
    """psycopg localizes timestamptz as Etc/UTC; canonicalize to UTC in place."""
    if frame.empty:
        return
    for column in columns:
        frame[column] = frame[column].dt.tz_convert("UTC")


class TimescaleBenchmarkStorage(BenchmarkStorage):
    """Persist benchmark artifacts in a forecast store.

    Backtest outputs are grouped by simulated ``available_at`` — one
    ``forecast.runs`` row per prediction event, points in ``forecasts``.
    "Overwrite gracefully" (base-class contract) is implemented as
    delete-then-insert for the run label: benchmark artifacts are workspace,
    not production history.

    Evaluation reports: metrics are snapshotted losslessly (pydantic JSON)
    into ``evaluation_runs.params`` and projected into
    ``evaluation_series``/``evaluation_metrics`` for querying; subset frames
    are re-derived on load from stored backtest output + ground truth via the
    same filtering operations the evaluation pipeline used. Re-saving appends
    a new evaluation run (a re-evaluation is a new belief, spec §7.5); load
    returns the latest.

    Analysis outputs (plots/HTML) are files, not rows: pass ``analysis_dir``
    to delegate them to openstef's local storage, or run with
    ``skip_analysis=True``.

    The store's declaration is the provider's unless ``store_config`` is
    given — read from the store's own ``store_tables`` on first use when
    neither declares one.
    """

    def __init__(
        self,
        source: ConnectionSource,
        run_name: str,
        target_provider: TimescaleTargetProvider,
        *,
        store_config: StoreConfig | None = None,
        model_label: str = "backtest",
        analysis_dir: Path | None = None,
        forecast_table: str = "forecasts",
    ) -> None:
        self._source = source
        self._run_name = run_name
        self._provider = target_provider
        # Declaration: explicit, else the provider's (same store) — read from
        # the store at the provider's schema on first use when neither has one.
        if store_config is not None:
            self._config, self._schema = store_config, store_config.schema
        else:
            self._config = target_provider.store_config
            self._schema = _schema_for(self._config, target_provider.store_schema)
        self._model = model_label
        # The workspace driver (spec §7.2): point benchmarks at a separate
        # forecast-log instance to keep experiment artifacts out of production
        # history (own retention/compression). One instance per storage object.
        self._forecast_table = forecast_table
        self._local = (
            LocalBenchmarkStorage(base_path=analysis_dir) if analysis_dir else None
        )

    # -- helpers -----------------------------------------------------------

    @contextmanager
    def _store(self) -> Iterator[Store]:
        """A Store for one call — its block is the transaction; the
        declaration, once resolved, is kept."""
        with Store.connect(self._source, self._config, schema=self._schema) as store:
            yield store
            self._config = store.config

    def _label(self, target: BenchmarkTarget) -> str:
        return f"{self._run_name}/{target.name}"

    def _series(self, target: BenchmarkTarget) -> str:
        return self._provider.measurement_series[target.name]

    def _has_runs(self, table: str, label: str) -> bool:
        with self._store() as store, store.conn.cursor() as cur:
            cur.execute(
                f"SELECT EXISTS (SELECT 1 FROM {store.schema}.{table} WHERE run_name = %s)",
                (label,),
            )
            return cur.fetchone()[0]

    # -- backtest outputs --------------------------------------------------

    def save_backtest_output(self, target: BenchmarkTarget, output: TimeSeriesDataset) -> None:
        frame = output.data
        quantile_cols = {
            col: quantile_column(Decimal(str(float(Quantile.parse(col)))))
            for col in frame.columns
            if Quantile.is_valid_quantile_string(col)
        }
        label = self._label(target)

        with self._store() as store:
            s = store.schema
            with store.conn.cursor() as cur:
                _allow_workspace_decompression(cur)
                # Overwrite gracefully: replace this label's previous artifacts.
                cur.execute(
                    f"DELETE FROM {s}.{self._forecast_table} WHERE run_id IN "
                    f"(SELECT run_id FROM {s}.runs WHERE run_name = %s)",
                    (label,),
                )
                cur.execute(f"DELETE FROM {s}.runs WHERE run_name = %s", (label,))

            for available_at, event_frame in frame.groupby("available_at"):
                points = [
                    (
                        ts.to_pydatetime(),
                        {dst: _noneify(row[src]) for src, dst in quantile_cols.items()},
                    )
                    for ts, row in event_frame.iterrows()
                ]
                store.write_forecast_run(
                    table=self._forecast_table,
                    series=self._series(target),
                    model=self._model,
                    run_name=label,
                    available_at=available_at.to_pydatetime(),
                    params={"benchmark_run": self._run_name, "target": target.name},
                    points=points,
                )
        # Leaving the block committed: the overwrite and every event's run, together.

    def load_backtest_output(self, target: BenchmarkTarget) -> TimeSeriesDataset:
        import pandas as pd

        label = self._label(target)

        with self._store() as store, store.conn.cursor() as cur:
            s = store.schema
            value_cols = _table_declaration(cur, s, self._forecast_table)["value_columns"]
            col_sql = "".join(f", f.{c}" for c in value_cols)
            series_id = store.get_series_id(self._series(target))
            cur.execute(
                f"SELECT sample_interval FROM {s}.series WHERE series_id = %s", (series_id,)
            )
            interval = cur.fetchone()[0]
            cur.execute(
                f"SELECT f.target_time, f.available_at{col_sql} "
                f"FROM {s}.{self._forecast_table} f JOIN {s}.runs r ON r.run_id = f.run_id "
                "WHERE r.run_name = %s AND f.series_id = %s "
                "ORDER BY f.target_time, f.available_at",
                (label, series_id),
            )
            rows = cur.fetchall()
        if not rows:
            raise KeyError(f"no backtest output stored for {label!r}")

        frame = pd.DataFrame(rows, columns=["timestamp", "available_at", *value_cols])
        _utc_normalize(frame, ("timestamp", "available_at"))
        frame = frame.dropna(axis="columns", how="all")
        renames = {
            col: Quantile(float(parse_quantile_column(col))).format()
            for col in frame.columns
            if col not in ("timestamp", "available_at", "mean")
        }
        frame = frame.rename(columns=renames).set_index("timestamp")
        return TimeSeriesDataset(data=frame, sample_interval=interval)

    def has_backtest_output(self, target: BenchmarkTarget) -> bool:
        return self._has_runs("runs", self._label(target))

    # -- evaluation outputs --------------------------------------------------

    def save_evaluation_output(self, target: BenchmarkTarget, output: EvaluationReport) -> None:
        from psycopg.types.json import Jsonb

        label = self._label(target)
        quantiles = sorted(
            {float(q) for report in output.subset_reports for q in report.subset.quantiles}
        )
        params = {
            "benchmark_run": self._run_name,
            "target": target.name,
            "target_column": self._provider.target_column,
            "quantiles": quantiles,
            "subsets": [
                {
                    "filtering": str(report.filtering),
                    "metrics": _METRICS_ADAPTER.dump_json(report.metrics).decode(),
                }
                for report in output.subset_reports
            ],
        }

        with self._store() as store:
            s = store.schema
            series_id = store.get_series_id(self._series(target))
            with store.conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {s}.evaluation_runs (run_name, params) "
                    "VALUES (%s, %s) RETURNING eval_run_id",
                    (label, Jsonb(params)),
                )
                eval_run_id = cur.fetchone()[0]

                series_cache: dict[tuple, int] = {}
                metric_rows = []
                for report in output.subset_reports:
                    filtering = str(report.filtering)
                    for metric in report.metrics:
                        win = "global" if metric.window == "global" else str(metric.window)
                        for qog, values in metric.metrics.items():
                            q_label = "global" if qog == "global" else str(float(qog))
                            for name, value in values.items():
                                key = (filtering, win, q_label, name)
                                if key not in series_cache:
                                    cur.execute(
                                        f"INSERT INTO {s}.evaluation_series "
                                        "(run_name, series_id, filtering, win, quantile, metric) "
                                        "VALUES (%s, %s, %s, %s, %s, %s) "
                                        "ON CONFLICT (run_name, series_id, filtering, win, quantile, metric) "
                                        "DO UPDATE SET run_name = EXCLUDED.run_name "
                                        "RETURNING eval_series_id",
                                        (label, series_id, *key[:3], name),
                                    )
                                    series_cache[key] = cur.fetchone()[0]
                                metric_rows.append(
                                    (series_cache[key], metric.timestamp, eval_run_id, _noneify(value))
                                )
                cur.executemany(
                    f"INSERT INTO {s}.evaluation_metrics (eval_series_id, ts, eval_run_id, value) "
                    "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    metric_rows,
                )
        # Leaving the block committed: evaluation run, series and metrics together.

    def load_evaluation_output(self, target: BenchmarkTarget) -> EvaluationReport:
        import pandas as pd

        label = self._label(target)
        with self._store() as store, store.conn.cursor() as cur:
            cur.execute(
                f"SELECT params FROM {store.schema}.evaluation_runs WHERE run_name = %s "
                "ORDER BY recorded_at DESC LIMIT 1",
                (label,),
            )
            row = cur.fetchone()
        if row is None:
            raise KeyError(f"no evaluation output stored for {label!r}")
        params = row[0]

        # Re-derive subsets from stored backtest output + ground truth using the
        # same operations EvaluationPipeline._iterate_subsets applied.
        predictions = self.load_backtest_output(target)
        ground_truth = self._provider.get_measurements_for_target(target)
        mask = self._provider.get_evaluation_mask_for_target(target)
        target_column = params["target_column"]
        quantiles = [Quantile(q) for q in params["quantiles"]]

        gt = ForecastInputDataset.from_timeseries(
            dataset=ground_truth.select_version(), target_column=target_column
        ).pipe_pandas(pd.DataFrame.dropna)

        subset_reports = []
        for entry in params["subsets"]:
            filtering = _FILTERING_ADAPTER.validate_python(entry["filtering"])
            metrics = _METRICS_ADAPTER.validate_json(entry["metrics"])
            if isinstance(filtering, AvailableAt):
                filtered = predictions.filter_by_available_at(available_at=filtering)
            else:
                filtered = predictions.filter_by_lead_time(lead_time=filtering)
            filtered = filtered.select_version()
            if mask is not None:
                filtered = filtered.filter_index(mask)
            if target_column in filtered.data.columns:
                filtered = filtered.pipe_pandas(lambda df: df.drop(columns=[target_column]))
            subset = ForecastDataset(
                data=gt.data.join(filtered.data, how="inner"),
                sample_interval=predictions.sample_interval,
                target_column=target_column,
            ).filter_quantiles(quantiles=quantiles)
            subset_reports.append(
                EvaluationSubsetReport(filtering=filtering, subset=subset, metrics=metrics)
            )
        return EvaluationReport(subset_reports=subset_reports)

    def has_evaluation_output(self, target: BenchmarkTarget) -> bool:
        return self._has_runs("evaluation_runs", self._label(target))

    # -- analysis outputs ----------------------------------------------------

    def save_analysis_output(self, output) -> None:  # noqa: ANN001
        if self._local is None:
            raise NotImplementedError(
                "analysis outputs are files; pass analysis_dir=... or run with skip_analysis=True"
            )
        self._local.save_analysis_output(output)

    def has_analysis_output(self, scope) -> bool:  # noqa: ANN001
        if self._local is None:
            return False
        return self._local.has_analysis_output(scope)


__all__ = ["TimescaleBenchmarkStorage", "TimescaleTargetProvider"]
