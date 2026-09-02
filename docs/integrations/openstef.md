# OpenSTEF Integration Reference

Status: implemented and live-tested (spec §10; validated 2026-08-25 against openstef 4.x
from PyPI and a Tiger Cloud TimescaleDB instance). This document is the adapter-level
reference — attach points, mappings, packaging notes, and reproduction commands — and the
substrate for the OpenSTEF-on-TimescaleDB tutorial. The convention itself lives in
[`../forecast-store-convention.md`](../forecast-store-convention.md).

---

## Why OpenSTEF

[OpenSTEF](https://github.com/OpenSTEF/openstef) (LF Energy) is the leading open-source
short-term energy forecasting pipeline, production-proven across thousands of grid
locations at the Dutch DSO Alliander. Version 4.x (current: v4.3.1) is a monorepo of five
packages (`openstef-core`, `openstef-models`, `openstef-beam`, `openstef-meta`,
`openstef-foundation-models`, the latter shipping Chronos-2 ONNX inference). Its docs are
explicit that **storage is the user's responsibility** — the 3.x database layer
(`openstef-dbc`) has no 4.x equivalent, and the migration guide's contract is "you own
I/O."

That makes it the ideal validation harness: an opinionated, production-proven pipeline *we
do not control*, with a storage-shaped hole. If the convention can back it cleanly, the
design demonstrably generalizes beyond our own examples.

## What the review of openstef@v4.3.1 established

- **OpenSTEF is bi-temporal in memory.** `TimeSeriesDataset` carries first-class
  `available_at`/`horizon` versioning with `filter_by_available_before()` and
  `select_version()`; the backtest engine enforces knowledge-time cutoffs on both training
  and prediction data via `RestrictedHorizonVersionedTimeSeries`. Its
  `(timestamp, available_at)` maps one-to-one onto the convention's
  `(target_time, available_at)` — the vocabulary is identical.
  But there is **no bi-temporal layer at rest**: every leakage guarantee rests on the
  caller having stamped `available_at` correctly, and nothing ships that stamps it.
- **Production forecasts carry no knowledge time.** `predict()` returns quantile columns
  on a timestamp index — no `available_at`, no horizon. Vintage stamping happens only
  inside the backtest loop; in production, recording knowledge time is left entirely to
  the caller. The store's write path is that recorder.
- **The benchmark exercises the convention hard.** The liander2024 public benchmark uses a
  7-quantile band (0.05–0.95), ships weather inputs as versioned vintage histories
  (`weather_forecasts_versioned/`), returns *measurements* versioned as well, and evaluates
  at `D-1T06:00` — a relative gate-closure cutoff of exactly the shape in the convention's
  evaluation join.
- Quantile columns are named `quantile_P{percent}` with fractional support
  (`quantile_P2.5`) — bijective with the convention's naming rule.
- Dataset metadata (`sample_interval`, column roles) travels in pandas `df.attrs`, which
  many operations drop, with a silent 15-minute default when absent — the failure the
  series registry exists to prevent.

## Attach points

OpenSTEF 4.x exposes three integration seams; the store implements adapters for the first
two and defers the third. Every non-deferred row is implemented and live-tested:

| OpenSTEF surface | Adapter | Role |
|---|---|---|
| `ForecastingCallback.on_predict_end` (openstef-models) | `ForecastStoreCallback` (`forecast_store.integrations.openstef`) | Production write path: stamps real `available_at`, writes `runs` + `forecasts` in one transaction. The only place production knowledge time gets recorded. |
| `TargetProvider` ABC (openstef-beam) | `TimescaleTargetProvider` (`forecast_store.integrations.openstef_beam`) | Data in: `actuals` → versioned measurements, `predictors` → versioned predictors, each series a lazy `data_part` of `VersionedTimeSeriesDataset`. Belief-log exports — the engine applies its own cutoffs. |
| `BenchmarkStorage` ABC (openstef-beam) | `TimescaleBenchmarkStorage` (`forecast_store.integrations.openstef_beam`) | Backtest outputs → `forecasts` with *simulated* `available_at`; `EvaluationReport` → `evaluation_runs` + `evaluation_series` + `evaluation_metrics`; `has_*_output` resume checks → indexed existence queries. |
| `examples/deployment` `services.py` stubs | `StoreReader` / `ForecastStoreCallback` | `fetch_load_measurements`/`fetch_weather_forecast` → store-served context reads (as-of covariates, gapfill on the declared grid); `publish_forecast` → the write path. Skeleton of the tutorial. |
| `VersionedTimeSeriesDataset` repository (openstef-core) | deferred | Pushdown of `filter_by_available_before` into SQL — an upstream-proposal candidate. |

**Run identity convention:** everything a benchmark writes for one target carries
`run_name = f"{benchmark_run}/{target.name}"` — the grouping label of spec §7.1, shared
between forecast runs and evaluation runs, and the key the resume checks probe.

## Type and semantics mapping

| OpenSTEF | Store |
|---|---|
| timestamp index | `target_time` |
| `available_at` | `available_at` — same name, same semantics |
| horizon (`LeadTime`) | derived: `target_time − available_at` |
| `sample_interval` (df.attrs) | `series.sample_interval` — registry authoritative; attrs never trusted |
| `quantile_P50` / `quantile_P2.5` | `q50` / `q02_5` |
| `ForecastDataset` | one wide row per `target_time` under a run |
| `select_version()` | the `DISTINCT ON` as-of query |
| `filter_by_available_before(t)` | `WHERE available_at <= t` |
| `stdev` column | not persisted (auxiliary; presence recorded in `runs.params`) |

## Configuration routing

OpenSTEF's `ForecastingWorkflowConfig` (~50 typed fields; the successor of 3.x's
`PredictionJobDataClass`) routes across the convention's three-way split (spec §7.4). The
adapter *hydrates* the OpenSTEF config from the store at run time:

| Field(s) | Destination |
|---|---|
| `model_id` | `runs.run_name` (doubles as MLflow model name) |
| `model` | `runs.model` |
| (trained artifact) | `runs.model_version` ← `MLFlowStorageCallback` |
| `quantiles` | caller config — SDK validates ⊆ band at write time |
| `sample_interval` | `series.sample_interval` (registry authoritative; conflict ⇒ error) |
| `horizons` | caller config; snapshotted in `runs.params` |
| `location.*` (coordinate, country_code) | `series.metadata` — adapter keys `location`, `country_code` |
| `target_column` + 6 × `*_column` | caller config: role→series-name bindings (the rename map; resolved via `get_series_id` at hydration) |
| `predict_history`, `cutoff_history`, `max_day_lags` | caller config; snapshotted in `runs.params` |
| completeness / flatliner gates | caller config; snapshotted in `runs.params` |
| feature engineering, `data_splitter`, sample weights, eval metrics | caller config; snapshotted in `runs.params` |
| `model_reuse_*`, `model_selection_*` | caller config; snapshotted in `runs.params` |
| `mlflow_storage` | deployment config — not the store |
| `run_name`, tags | `runs.params` |

Benchmark target metadata (`BenchmarkTarget`) splits the same way: identity, description,
coordinates, capacity limits, and the category label are series facts (registry — typed
columns or `metadata` keys, with `group_name` round-tripping through
`metadata->'tags'->>'group'`); `train_start`/`benchmark_*` are experiment scope and stay
in harness arguments.

## Adapter-level lessons

- **`context_end` from engine-fed frames.** OpenSTEF prediction inputs carry future
  covariate timestamps (weather beyond `forecast_start`) — target times, not knowledge
  times. `ForecastStoreCallback` therefore derives `context_end` from the last *observed
  target* value (max index would false-positive the leakage audit on every run) and
  records the method in `runs.params`. Store-served context (`StoreReader`) completes the
  audit's covariate half via the recorded `covariates_asof` (spec §9.3).
- **Evaluation reports round-trip via re-derivation.** Metrics are snapshotted losslessly
  (pydantic JSON) in `evaluation_runs.params` and projected relationally; subset frames
  are re-derived on load from stored backtest output + ground truth using the pipeline's
  own filtering operations. Auxiliary backtest columns (the target copy, `stdev`) do not
  round-trip — the target rejoins from `actuals`.
- **Workspace instance.** `TimescaleBenchmarkStorage(forecast_table="bt_workspace")`
  writes benchmark artifacts into a separate forecast-log instance (declared via
  `StoreConfig.with_tables(...)`), keeping experiment churn out of production forecast
  history with its own retention/compression. One instance per storage object.
- **Benchmark overwrite = label-scoped replace.** The `BenchmarkStorage` contract requires
  graceful overwrite; implemented as delete-then-insert scoped to the run label, per the
  convention's benchmark-workspace rule (spec §4.1). Evaluation re-saves append.
- **Backtests churn compressed history.** Backtest writes and label-scoped overwrites land
  in old `target_time` regions — exactly where the columnstore policy has compressed — and
  trip TimescaleDB's DML decompression limit. Workspace transactions therefore lift it
  locally (`SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0`);
  production paths never need this, which is itself a useful invariant.

## Packaging notes

- OpenSTEF's preset factory imports xgboost at import time regardless of the configured
  model → install `openstef-models[xgb-cpu]`.
- Chronos-2 needs `openstef-foundation-models[cpu]` for onnxruntime.
- The workflow config's default MLflow file store trips modern MLflow — pass
  `mlflow_storage=None` for benchmark runs.

## Open items (adapter-level)

Convention-level questions live in the spec's §11; these are integration follow-ups:

- **Ensemble member persistence needs a hook.** `on_predict_end` receives only the final
  combined `ForecastDataset`; the per-learner `EnsembleForecastDataset` is internal to the
  model. Persisting members under the spec's candidate mapping (one run per producer)
  requires an ensemble-aware callback or an upstream hook — investigate when porting the
  ensemble benchmark.
- **Port the remaining liander benchmarks** — the ensemble run (exercises the
  multi-producer question) and the XGBoost/GBLinear runs (the TSFM-vs-classical
  comparison on identical store-served data).
- **Upstream path.** The deferred `VersionedTimeSeriesDataset` pushdown repository is the
  natural upstream-proposal candidate; offering `TimescaleBenchmarkStorage` /
  `TimescaleTargetProvider` upstream as an official backend waits on tutorial traction.
- **Tutorial — done.** [`docs/tutorials/openstef-on-timescaledb.md`](../tutorials/openstef-on-timescaledb.md)
  narrates the ingest + benchmark scripts and the `StoreReader`/`ForecastStoreCallback`
  production path, with live-verified commands and outputs.

## Reproducing the liander Chronos-2 run

```bash
# ingest one benchmark target (measurements with real per-row claims + weather vintages)
uv run --extra openstef --extra foundation python scripts/ingest_liander.py

# run OpenSTEF's own Chronos-2 benchmark wiring against the store
uv run --extra openstef --extra foundation python scripts/run_liander_chronos2.py
```

First recorded run (wind park Arnhem-Nijmegen normalized, 7 benchmark days, 28 events,
3-day horizon at 15-minute resolution): 35,133 measurements + 591k weather vintage rows
ingested; 8,092 forecast points across 28 simulated vintages; **rCRPS 0.0906,
rMAE@q50 0.1248**; observed-probability calibration near-nominal across the 7-level band
(q05→0.076 … q95→0.995). The script's summary is a SQL query against
`evaluation_series`/`evaluation_metrics`.

**Full official-window comparison** (2026-08-26; all 5 wind parks × 306 days,
`scripts/run_liander_benchmark.py`, four models on identical store-served data —
24,448 vintages, 10.5M forecast points; averages across parks, global window):

| model | avg rCRPS | avg rMAE@q50 |
|---|---|---|
| chronos2-base (zero-shot) | **0.0726** | **0.1016** |
| chronos2-small (zero-shot) | 0.0739 | 0.1036 |
| xgboost (weekly retrain) | 0.0946 | 0.1107 |
| gblinear (weekly retrain) | 0.0947 | 0.1312 |

Zero-shot TSFMs beat the trained classical presets by ~23% rCRPS; xgboost and gblinear
tie on rCRPS while diverging on the median. Operational note: a full-window benchmark
leaves partial chunks and dead index entries at gigabyte scale — run the recompress +
reindex hygiene pass afterward (this run: 4.2 GB → 738 MB).

Live tests: `tests/test_openstef_callback.py`, `tests/test_context_reads.py`,
`tests/test_benchmark_pipeline.py` (gated on `FORECAST_STORE_TEST_DSN`).
