# forecast-store

**A forecast store is a tri-temporal store for forecasts** — and for the data
they are made from and scored against. Producing a forecast has become nearly
free; *operating* forecasts — keeping every vintage, guaranteeing
point-in-time correctness, reproducing backtests, tracking accuracy, catching
drift — is hand-rolled glue code on every team that ships one. This repo is an
open schema convention for that layer on Postgres/TimescaleDB, plus its
reference implementation: a schema generator, a small SDK, and adapters.

Two ideas carry the whole design:

- **Forecasts are tri-temporal.** Every row answers three questions with three
  clocks: when is the value *true* (`target_time`), when did its source *know*
  it (`available_at`), and when did your database *learn* it (`recorded_at`,
  stamped by the database, never by a client). Cut `available_at` at a
  simulated decision moment and backtests cannot leak — the query cannot
  return what wasn't knowable. Pin `recorded_at` and evaluations reproduce
  exactly, forever.
- **A forecast store is a log of beliefs, not a table of values.** Measurements
  and predictions are different kinds of belief and live in different
  append-only tables; a revision or a new vintage is a new row, never an
  update.

| table | holds | a newer row about the same target |
|---|---|---|
| `actuals` | measurements, possibly revised | corrects it |
| `predictors` | external forecast vintages (weather, prices) | re-predicts it |
| `forecasts` | your models' output, with run provenance (`runs`) | re-predicts it |
| `evaluation_*` | accuracy results | is a new evaluation |

## The operational questions, answered

| you ask | the store answers with |
|---|---|
| How good is the model? | a backtest that cuts `available_at <= S` — it *cannot* leak, because the query cannot return what wasn't knowable |
| How good is the model *as operated*? | pin `recorded_at <= S` as well — scoring against what your system had actually ingested, delays included |
| Can I reproduce last quarter's evaluation? | freeze `recorded_at` at experiment time; the same rows come back forever, whatever arrived since |
| What did we know when that bid went wrong? | one as-of read at bid time — separating *the model was wrong* from *the inputs were late or later revised* |
| Is the model drifting? | accuracy lives as rows in `evaluation_*`; drift is a `WHERE` clause and a dashboard, not a notebook |
| Which vendor forecasts best at a 12-hour lead? | every superseded vintage stays scoreable — vendor skill by lead time is a query |
| Did the feed deliver on time? | `recorded_at − available_at` is the *measured* delivery lag, per row |
| Can I backfill years of history honestly? | state genuine per-row availability and backtests behave as if you'd ingested live; a claimless load fails loud, never quietly optimistic |

## Quickstart

```bash
pip install forecast-store        # v0 in preparation; see status below
forecast-store ddl                # print the generated schema
forecast-store provision --dsn postgres://...
forecast-store provision --dsn postgres://... --config store.yaml   # any set of tables, from a YAML declaration
forecast-store register-series site42/load --interval "15 minutes" --dsn postgres://...
forecast-store describe --dsn postgres://...                        # the store's declaration, as a YAML file
forecast-store describe --dsn postgres://... --config store.yaml    # drift check: exit 1 if the store differs
```

Write a forecast with honest knowledge time, then read it back as-of any
moment:

```python
from forecast_store import Store

with Store.connect(DSN) as store:            # a DSN or a pool; declaration read from the store
    store.register_series("site42/load", "15 minutes")   # get-or-create
    store.write_forecast(
        series="site42/load",                 # a registered name, or its series_id
        model="my-model", run_name="prod/site42",
        available_at=now,                     # the knowledge time, recorded
        points=[(ts, {"q05": 1.1, "q50": 2.0, "q95": 3.2}), ...],
    )
    # Same point shape for measurements and vendor feeds; knowledge time follows the table's role:
    store.write_actuals("site42/load", [(ts, 2.1), ...])                  # arrival measured
    store.write_predictors("site42/wx/temp", [(ts, 9.8), ...], available_at=published)
# the block is the unit of work: committed on exit, rolled back on exception
```

`Store` is sugar over the module functions — `write_forecast(conn, config, ...)` and
friends take a connection and a declaration you manage, and never commit. Bind a connection
you already hold (or a pool checkout) with `Store(conn, config)`; `StoreConfig.from_store(conn)`
reads the declaration a store was provisioned with.

```sql
-- The workhorse query: the latest belief per target, as of a knowledge cutoff.
SELECT DISTINCT ON (target_time) target_time, available_at, q50
FROM forecast.forecasts
WHERE series_id = forecast.get_series_id('site42/load')
  AND target_time BETWEEN %(t0)s AND %(t1)s
  AND available_at <= %(asof)s
ORDER BY target_time, available_at DESC;
```

## OpenSTEF integration

Adapters for every integration seam [OpenSTEF](https://github.com/OpenSTEF/openstef) 4.x
exposes (`forecast_store.integrations`):

| OpenSTEF seam | object | role |
|---|---|---|
| `ForecastingCallback` | `ForecastStoreCallback` | persists every `predict()` with its real `available_at` — the knowledge time OpenSTEF itself never records; run + points in one transaction |
| context assembly | `StoreReader` (with `Observed` / `ForecastFeed` bindings) | model-ready datasets: measured history to the decision moment, covariates as-of it — leakage-free by construction, read provenance riding along as `store_context` |
| `TargetProvider` | `TimescaleTargetProvider` | serves versioned measurements and predictor vintages to OpenSTEF's backtest engine, which applies its own knowledge cutoffs |
| `BenchmarkStorage` | `TimescaleBenchmarkStorage` | backtest vintages (simulated `available_at`) and evaluation reports into the store; label-scoped overwrite, indexed resume checks, per-instance target tables |

The production loop in miniature — context in as-of the decision moment, forecasts out
with honest knowledge time:

```python
from forecast_store.integrations.openstef import ForecastStoreCallback, StoreReader
from openstef_models.workflows.custom_forecasting_workflow import CustomForecastingWorkflow

dataset = StoreReader(DSN).context(
    target_series="site42/load",
    covariates={"wind_speed_80m": "site42/wx/wind_speed_80m"},  # plain name = vendor feed
    history_start=asof - timedelta(days=30), asof=asof, horizon_end=asof + timedelta(days=3),
)
workflow = CustomForecastingWorkflow(
    model=model, model_id="site42",
    callbacks=[ForecastStoreCallback(DSN, "site42/load")],
)
workflow.fit(dataset)
workflow.predict(dataset)  # persisted: run + points, real available_at, one transaction
```

Every adapter takes a connection source — a DSN, or a pool with a `.connection()` context
manager — and works in one connection per call. The store's declaration is read from its own
`store_tables` on first use when `store_config` is omitted; pass `schema=` (or `store_schema=`
on the provider) for a store outside the default `forecast` schema.

Details, mappings, and packaging notes: [`docs/integrations/openstef.md`](docs/integrations/openstef.md).

## Validated against a real pipeline

The convention is validated end-to-end against
[OpenSTEF](https://github.com/OpenSTEF/openstef) (LF Energy's short-term
energy forecasting pipeline), with the store as the *only* data source and
result sink for its public liander2024 benchmark — real grid measurements with
their real ~48-hour publication lags, weather as versioned vintage histories.
Full official window, 5 wind parks × 306 days, identical store-served data:

| model | weather covariates | avg rCRPS | avg rMAE@q50 |
|---|---|---|---|
| Chronos-2 base (zero-shot) | all 11 | **0.0711** | **0.0987** |
| Chronos-2 base (zero-shot) | 3 (official example) | 0.0726 | 0.1016 |
| Chronos-2 small (zero-shot) | 3 | 0.0739 | 0.1036 |
| XGBoost + conformal calibration | all 11 + engineered | 0.0834 | 0.1126 |
| TimesFM 2.5 (zero-shot)\* | all 11, via XReg | 0.0848 | 0.1116 |
| TimesFM 2.5 (zero-shot)\* | 3, via XReg | 0.0894 | 0.1185 |
| XGBoost (weekly retrain) | all 11 + engineered | 0.0946 | 0.1107 |
| GBLinear (weekly retrain) | all 11 + engineered | 0.0947 | 0.1312 |
| Moirai 2.0 R small (zero-shot)\* | all 11 | 0.1416 | 0.1868 |
| Moirai 2.0 R small (zero-shot)\* | 3 | 0.1421 | 0.1856 |
| TimesFM 2.5 (zero-shot)\* | none (univariate) | 0.1437 | 0.1928 |

Two findings carry the table: input access moves TimesFM 41%
(0.1437 → 0.0848) through a *linear* side-channel, and conformal
calibration alone closes half of the classical presets' gap (rCRPS −12%,
median untouched) — most of what separated the middle of this table was
covariate access and band calibration, not model class. The store is what
makes the comparison honest, serving every model identical versioned
vintages.

\* Decile-native models: scored over their own 5-level band (a separate
forecast-log instance whose declared band is true), so their rCRPS spans a
narrower band than the others'. Every number above is a row in the store's
evaluation tables; the full record — per-park results, run labels, and the
planned runs — is [`docs/benchmark_log.md`](docs/benchmark_log.md).

## Documentation

- **The convention** — the spec this package generates:
  [`docs/forecast-store-convention.md`](docs/forecast-store-convention.md)
- **Design rationale** — alternatives considered and worked examples, per rule:
  [`docs/forecast-store-rationale.md`](docs/forecast-store-rationale.md)
- **Tutorial** — OpenSTEF on TimescaleDB in six steps:
  [`docs/tutorials/openstef-on-timescaledb.md`](docs/tutorials/openstef-on-timescaledb.md)
- **OpenSTEF adapter reference**:
  [`docs/integrations/openstef.md`](docs/integrations/openstef.md)
- **Benchmark log** — every run, its results, and what's planned:
  [`docs/benchmark_log.md`](docs/benchmark_log.md)

## Requirements and status

Built for TimescaleDB (hypertables, columnstore, and the generated
data-quality sweep); the core schema and read/write paths also run on plain
Postgres 14+. Python ≥ 3.12.

**Status: pre-release.** The convention is draft v0.4 (spike-validated; the
spec's §11 keeps an honest ledger of open and closed design questions). The
PyPI name currently holds a placeholder; the v0 package release is in
preparation, and APIs may still change.

## License

Apache-2.0
