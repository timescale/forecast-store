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
```

Write a forecast with honest knowledge time, then read it back as-of any
moment:

```python
import psycopg
from forecast_store.config import StoreConfig
from forecast_store.write import write_forecast_run

config = StoreConfig()  # 7-level quantile band by default; fully declarable
with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT forecast.register_series('site42/load', interval '15 minutes')")
        series_id = cur.fetchone()[0]
    write_forecast_run(
        conn, config, series_id=series_id,
        model="my-model", run_name="prod/site42",
        available_at=now,                       # the knowledge time, recorded
        points=[(ts, {"q05": 1.1, "q50": 2.0, "q95": 3.2}), ...],
    )
    conn.commit()  # run + points: one transaction
```

```sql
-- The workhorse query: the latest belief per target, as of a knowledge cutoff.
SELECT DISTINCT ON (target_time) target_time, available_at, q50
FROM forecast.forecasts
WHERE series_id = forecast.get_series_id('site42/load')
  AND target_time BETWEEN %(t0)s AND %(t1)s
  AND available_at <= %(asof)s
ORDER BY target_time, available_at DESC;
```

## Validated against a real pipeline

The convention is validated end-to-end against
[OpenSTEF](https://github.com/OpenSTEF/openstef) (LF Energy's short-term
energy forecasting pipeline), with the store as the *only* data source and
result sink for its public liander2024 benchmark — real grid measurements with
their real ~48-hour publication lags, weather as versioned vintage histories.
Full official window, 5 wind parks × 306 days, identical store-served data:

| model | avg rCRPS | avg rMAE@q50 |
|---|---|---|
| Chronos-2 base (zero-shot) | **0.0726** | **0.1016** |
| Chronos-2 small (zero-shot) | 0.0739 | 0.1036 |
| XGBoost (weekly retrain) | 0.0946 | 0.1107 |
| GBLinear (weekly retrain) | 0.0947 | 0.1312 |

The harness (`scripts/run_liander_benchmark.py`) also wraps TimesFM 2.5 and
Moirai 2.0. Every number above is a row in the store's evaluation tables.

## Documentation

- **The convention** — the spec this package generates:
  [`docs/forecast-store-convention.md`](docs/forecast-store-convention.md)
- **Tutorial** — OpenSTEF on TimescaleDB in six steps:
  [`docs/tutorials/openstef-on-timescaledb.md`](docs/tutorials/openstef-on-timescaledb.md)
- **OpenSTEF adapter reference**:
  [`docs/integrations/openstef.md`](docs/integrations/openstef.md)

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
